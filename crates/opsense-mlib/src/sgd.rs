//! # SGD Optimizer — Stochastic Gradient Descent cho black-box functions
//!
//! Dùng finite difference để ước lượng gradient vì objective function
//! (backtest) không khả vi.
//!
//! Hỗ trợ:
//! - Parameter bounds `[min, max]`
//! - Integer parameters (round sau mỗi update)
//! - Momentum
//! - Learning rate decay
//!
//! # Parallel gradient estimation
//!
//! [`estimate_gradient`] dùng `tokio::spawn` để farm từng dimension
//! evaluation ra thread pool, tận dụng đa core thật sự. Bound yêu cầu
//! `Fn + Sync + 'static` — closure được bọc trong `Arc` trước khi truyền
//! xuống.
//!
//! # Cấu trúc
//!
//! - [`SGDOptimizer`] — struct cấu hình, implement [`SgdStep`] trait
//! - [`SgdStep`] — trait tách optimization thành các giai đoạn:
//!   [`init`](SgdStep::init) → [`gradient`](SgdStep::gradient) →
//!   [`step`](SgdStep::step) (lặp) → [`converged`](SgdStep::converged)
//! - [`SgdState`] — state hiện tại (params, velocity, history)

use async_trait::async_trait;
use std::sync::Arc;

/// State của SGD optimizer tại một thời điểm trong quá trình optimize.
#[derive(Debug, Clone)]
pub struct SgdState {
    /// Current parameter values.
    pub params: Vec<f64>,
    /// Momentum buffer (velocity).
    pub velocity: Vec<f64>,
    /// Objective value history (mỗi epoch một entry).
    pub history: Vec<f64>,
}

/// Trait tách optimization thành các giai đoạn riêng biệt.
///
/// Mọi method đều async, dùng `tokio::spawn` cho gradient estimation
/// song song. Objective cần `Fn + Sync + 'static`.
#[async_trait]
pub trait SgdStep {
    /// Khởi tạo state từ `initial` params.
    fn init(&self, initial: &[f64]) -> SgdState;

    /// Tính gradient của `objective` tại `params` (song song).
    async fn gradient<F, Fut>(&self, objective: &Arc<F>, params: &[f64]) -> Vec<f64>
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send;

    /// Một bước SGD (async).
    async fn step<F, Fut>(&self, objective: &Arc<F>, state: &mut SgdState) -> f64
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send;

    /// Kiểm tra hội tụ: `|current_obj - prev_obj| < tol`.
    fn converged(&self, prev_obj: f64, current_obj: f64) -> bool;
}

// ── SGDOptimizer ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SGDOptimizer {
    lr: f64,
    momentum: f64,
    max_epochs: usize,
    tol: f64,
    epsilon: f64,
    lr_decay: f64,
    grad_clip: f64,
    bounds: Vec<(f64, f64)>,
    is_integer: Vec<bool>,
}

impl Default for SGDOptimizer {
    fn default() -> Self {
        Self::new(1)
    }
}

impl SGDOptimizer {
    pub fn new(n: usize) -> Self {
        Self {
            lr: 0.01,
            momentum: 0.9,
            max_epochs: 1000,
            tol: 1e-6,
            epsilon: 0.001,
            lr_decay: 0.0,
            grad_clip: 0.0,
            bounds: vec![(f64::NEG_INFINITY, f64::INFINITY); n],
            is_integer: vec![false; n],
        }
    }

    pub fn with_bounds(mut self, i: usize, min: f64, max: f64) -> Self {
        if let Some(b) = self.bounds.get_mut(i) {
            *b = (min, max);
        }
        self
    }

    pub fn with_integer(mut self, i: usize) -> Self {
        if let Some(v) = self.is_integer.get_mut(i) {
            *v = true;
        }
        self
    }

    pub fn with_lr(mut self, lr: f64) -> Self {
        self.lr = lr;
        self
    }

    pub fn with_momentum(mut self, m: f64) -> Self {
        self.momentum = m;
        self
    }

    pub fn with_max_epochs(mut self, n: usize) -> Self {
        self.max_epochs = n;
        self
    }

    pub fn with_tol(mut self, tol: f64) -> Self {
        self.tol = tol;
        self
    }

    pub fn with_epsilon(mut self, eps: f64) -> Self {
        self.epsilon = eps;
        self
    }

    pub fn with_lr_decay(mut self, decay: f64) -> Self {
        self.lr_decay = decay.clamp(0.0, 0.5);
        self
    }

    pub fn with_grad_clip(mut self, threshold: f64) -> Self {
        self.grad_clip = threshold.max(0.0);
        self
    }

    // ── Getters ──

    pub fn lr(&self) -> f64 {
        self.lr
    }
    pub fn momentum(&self) -> f64 {
        self.momentum
    }
    pub fn max_epochs(&self) -> usize {
        self.max_epochs
    }
    pub fn epsilon(&self) -> f64 {
        self.epsilon
    }
    pub fn bounds(&self) -> &[(f64, f64)] {
        &self.bounds
    }
    pub fn is_integer(&self) -> &[bool] {
        &self.is_integer
    }

    // ── Helpers (private) ──

    /// Đánh giá `f` tại `params` và tất cả các dimension perturbation
    /// đồng thời qua `tokio::spawn`. Mỗi dimension spawn 1 task riêng →
    /// chạy song song thật sự trên thread pool multi-core.
    async fn estimate_gradient<F, Fut>(&self, f: Arc<F>, params: &[f64]) -> Vec<f64>
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send,
    {
        let n = params.len();
        let base = f(params).await;

        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let eps = if self.is_integer[i] {
                self.epsilon.max(0.5)
            } else {
                self.epsilon
            };

            let mut plus = params.to_vec();
            plus[i] += eps;
            plus[i] = clip_param(plus[i], self.bounds[i]);

            let mut minus = params.to_vec();
            minus[i] -= eps;
            minus[i] = clip_param(minus[i], self.bounds[i]);

            let actual_eps = plus[i] - minus[i];

            let f_clone = Arc::clone(&f);
            handles.push(tokio::spawn(async move {
                let f_plus = f_clone(&plus).await;
                let f_minus = f_clone(&minus).await;
                (i, f_plus, f_minus, actual_eps, eps)
            }));
        }

        let mut grad = vec![0.0f64; n];
        for handle in handles {
            let (i, f_plus, f_minus, actual_eps, eps) = handle.await.unwrap();
            if actual_eps > 1e-12 {
                grad[i] = (f_plus - f_minus) / actual_eps;
            } else {
                grad[i] = (f_plus - base) / eps;
            }
        }

        grad
    }

    /// Full optimization loop.
    ///
    /// Gradient estimation dùng `tokio::spawn` để farm mỗi dimension
    /// evaluation ra thread pool → chạy song song thật sự trên multi-core.
    /// Objective cần `'static` — được bọc trong `Arc` ở đầu hàm.
    pub async fn optimize<F, Fut>(&self, objective: F, initial: &[f64]) -> (Vec<f64>, Vec<f64>)
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send,
    {
        let objective = Arc::new(objective);
        let mut state = self.init(initial);
        let mut prev_obj = f64::NEG_INFINITY;

        for _ in 0..self.max_epochs {
            let current_obj = self.step(&objective, &mut state).await;
            if self.converged(prev_obj, current_obj) {
                break;
            }
            prev_obj = current_obj;
        }

        (state.params, state.history)
    }
}

#[async_trait]
impl SgdStep for SGDOptimizer {
    fn init(&self, initial: &[f64]) -> SgdState {
        assert_eq!(initial.len(), self.bounds.len(), "params dim mismatch");

        let mut params = initial.to_vec();
        for (i, item) in params.iter_mut().enumerate() {
            *item = clip_param(*item, self.bounds[i]);
            if self.is_integer[i] {
                *item = item.round();
                *item = clip_param(*item, self.bounds[i]);
            }
        }

        SgdState {
            params,
            velocity: vec![0.0f64; initial.len()],
            history: Vec::with_capacity(self.max_epochs),
        }
    }

    async fn gradient<F, Fut>(&self, objective: &Arc<F>, params: &[f64]) -> Vec<f64>
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send,
    {
        self.estimate_gradient(Arc::clone(objective), params).await
    }

    async fn step<F, Fut>(&self, objective: &Arc<F>, state: &mut SgdState) -> f64
    where
        F: Fn(&[f64]) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = f64> + Send,
    {
        let grad = self.gradient(objective, &state.params).await;

        let grad = if self.grad_clip > 0.0 {
            grad.iter()
                .map(|g| g.clamp(-self.grad_clip, self.grad_clip))
                .collect::<Vec<_>>()
        } else {
            grad
        };

        let lr = self.lr * (1.0 - self.lr_decay).powi(state.history.len() as i32);

        for (i, (param, velocity)) in state
            .params
            .iter_mut()
            .zip(state.velocity.iter_mut())
            .enumerate()
        {
            *velocity = self.momentum * *velocity + lr * grad[i];
            *param += *velocity;
            *param = clip_param(*param, self.bounds[i]);

            if self.is_integer[i] {
                *param = param.round();
                *param = clip_param(*param, self.bounds[i]);
            }
        }

        let obj = objective(&state.params).await;
        state.history.push(obj);
        obj
    }

    fn converged(&self, prev_obj: f64, current_obj: f64) -> bool {
        if prev_obj == f64::NEG_INFINITY {
            return false;
        }
        (current_obj - prev_obj).abs() < self.tol
    }
}

fn clip_param(v: f64, (lo, hi): (f64, f64)) -> f64 {
    v.clamp(lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Basic behavior (không cần async) ──────────────────────────

    #[test]
    fn test_sgd_step_init() {
        let opt = SGDOptimizer::new(2)
            .with_bounds(0, 0.0, 10.0)
            .with_bounds(1, -5.0, 5.0);

        let state = opt.init(&[5.0, 0.0]);

        assert_eq!(state.params.len(), 2);
        assert_eq!(state.params[0], 5.0);
        assert_eq!(state.params[1], 0.0);
        assert_eq!(state.velocity, vec![0.0, 0.0]);
        assert!(state.history.is_empty());
    }

    #[test]
    fn test_sgd_step_init_clamp() {
        let opt = SGDOptimizer::new(1).with_bounds(0, 0.0, 10.0);
        let state = opt.init(&[20.0]);
        assert!((state.params[0] - 10.0).abs() < 1e-10);
    }

    #[test]
    fn test_sgd_step_converged_check() {
        let opt = SGDOptimizer::new(1);
        assert!(!opt.converged(f64::NEG_INFINITY, 0.0));
        assert!(opt.converged(1.0, 1.0 + 1e-10));
        assert!(!opt.converged(1.0, 2.0));
    }

    // ── Async optimization ─────────────────────────────────────────

    #[tokio::test]
    async fn test_optimize_quadratic() {
        let opt = SGDOptimizer::new(1)
            .with_bounds(0, -10.0, 10.0)
            .with_lr(0.1)
            .with_max_epochs(200)
            .with_tol(1e-8);

        let (best, history) = opt
            .optimize(
                |p| {
                    let x = p[0];
                    async move { -(x - 3.0).powi(2) + 10.0 }
                },
                &[0.0],
            )
            .await;

        assert!((best[0] - 3.0).abs() < 0.05, "got {}", best[0]);
        assert!(*history.last().unwrap() > 9.99);
    }

    #[tokio::test]
    async fn test_optimize_two_params() {
        let opt = SGDOptimizer::new(2)
            .with_bounds(0, -5.0, 5.0)
            .with_bounds(1, -5.0, 5.0)
            .with_lr(0.1)
            .with_max_epochs(300)
            .with_tol(1e-8);

        let (best, history) = opt
            .optimize(
                |p| {
                    let x = p[0];
                    let y = p[1];
                    async move { -(x - 2.0).powi(2) - (y + 1.0).powi(2) + 10.0 }
                },
                &[0.0, 0.0],
            )
            .await;

        assert!((best[0] - 2.0).abs() < 0.1, "got {}", best[0]);
        assert!((best[1] + 1.0).abs() < 0.1, "got {}", best[1]);
        assert!(*history.last().unwrap() > 9.9);
    }

    #[tokio::test]
    async fn test_optimize_integer_param() {
        let opt = SGDOptimizer::new(1)
            .with_bounds(0, 1.0, 20.0)
            .with_integer(0)
            .with_lr(0.5)
            .with_momentum(0.0)
            .with_max_epochs(100);

        let (best, _) = opt
            .optimize(
                |p| {
                    let x = p[0];
                    async move { -(x - 7.0).powi(2) + 5.0 }
                },
                &[1.0],
            )
            .await;

        assert_eq!(best[0] as i32, 7, "got {}", best[0]);
    }

    #[tokio::test]
    async fn test_optimize_bounds_clamp() {
        let opt = SGDOptimizer::new(1)
            .with_bounds(0, 0.0, 5.0)
            .with_lr(0.5)
            .with_max_epochs(50);

        let (best, _) = opt
            .optimize(
                |p| {
                    let x = p[0];
                    async move { x }
                },
                &[0.0],
            )
            .await;

        assert!((best[0] - 5.0).abs() < 0.01, "got {}", best[0]);
    }

    #[tokio::test]
    async fn test_optimize_lr_decay() {
        let opt = SGDOptimizer::new(1)
            .with_bounds(0, -10.0, 10.0)
            .with_lr(0.5)
            .with_momentum(0.0)
            .with_lr_decay(0.02)
            .with_max_epochs(50);

        let (best, _) = opt
            .optimize(
                |p| {
                    let x = p[0];
                    async move { -(x - 3.0).powi(2) + 10.0 }
                },
                &[0.0],
            )
            .await;

        assert!((best[0] - 3.0).abs() < 0.1);
    }

    // ── Gradient estimation ────────────────────────────────────────

    #[tokio::test]
    async fn test_gradient_three_params() {
        // f(x,y,z) = -(x-1)² - (y+2)² - (z-3)² + 20
        // gradient at (0,0,0) = (2, -4, 6)
        let opt = SGDOptimizer::new(3)
            .with_bounds(0, -5.0, 5.0)
            .with_bounds(1, -5.0, 5.0)
            .with_bounds(2, -5.0, 5.0)
            .with_epsilon(0.01);

        let f = |p: &[f64]| {
            let x = p[0];
            let y = p[1];
            let z = p[2];
            async move { -(x - 1.0).powi(2) - (y + 2.0).powi(2) - (z - 3.0).powi(2) + 20.0 }
        };
        let f_arc = Arc::new(f);
        let grad = opt.gradient(&f_arc, &[0.0, 0.0, 0.0]).await;

        assert!(
            (grad[0] - 2.0).abs() < 0.02,
            "grad[0]={}, expected ~2",
            grad[0]
        );
        assert!(
            (grad[1] - (-4.0)).abs() < 0.02,
            "grad[1]={}, expected ~-4",
            grad[1]
        );
        assert!(
            (grad[2] - 6.0).abs() < 0.02,
            "grad[2]={}, expected ~6",
            grad[2]
        );
    }

    // ── Manual step loop ───────────────────────────────────────────

    #[tokio::test]
    async fn test_step_manual_loop() {
        let opt = SGDOptimizer::new(1)
            .with_bounds(0, -10.0, 10.0)
            .with_lr(0.1)
            .with_momentum(0.0)
            .with_max_epochs(100)
            .with_tol(1e-8);

        let f = |p: &[f64]| {
            let x = p[0];
            async move { -(x - 3.0).powi(2) + 10.0 }
        };
        let f_arc = Arc::new(f);

        let mut state = opt.init(&[0.0]);
        let mut prev = f64::NEG_INFINITY;

        for _ in 0..200 {
            let obj = opt.step(&f_arc, &mut state).await;
            if opt.converged(prev, obj) {
                break;
            }
            prev = obj;
        }

        assert!(
            (state.params[0] - 3.0).abs() < 0.05,
            "got {}",
            state.params[0]
        );
        assert!(*state.history.last().unwrap() > 9.99);
    }
}
