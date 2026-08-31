//! Random Cut Forest (RCF) — phát hiện bất thường trên luồng dữ liệu.
//!
//! Cài đặt Robust Random Cut Forest (Guha et al. 2016) kiểu streaming:
//! - Rừng gồm `num_trees` cây Random Cut Tree, mỗi cây giữ một mẫu trượt
//!   tối đa `sample_size` điểm (FIFO eviction).
//! - Điểm tới được chèn vào từng cây; điểm chèn xong được chấm điểm
//!   collusive displacement: với mỗi ancestor trên đường đi, tỉ số
//!   `log(size(node)) / log(max(size(node_chứa_điểm), size(sibling)) + 1)`,
//!   lấy max theo đường rồi lấy trung bình trên rừng. Điểm thường ~1.0,
//!   điểm bất thường (nằm một mình trong vùng thưa) cho điểm lớn dần.
//! - Hỗ trợ shingling: `shingle_size > 1` ghép `k` điểm liên tiếp thành
//!   một vector để bắt bất thường theo *hình dạng* chuỗi, không chỉ giá trị.
//!
//! Không phụ thuộc crate ngoài (PRNG xorshift nội bộ), dùng cho script
//! phân tích trong `opsense-rhai` cũng như Rust code khác.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Cấu hình rừng RCF ([`RcfForest::with_config`]).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RcfConfig {
    /// Số cây trong rừng. Nhiều cây → điểm mượt hơn, chậm hơn tuyến tính.
    pub num_trees: usize,
    /// Kích thước mẫu trượt mỗi cây (số điểm tối đa giữ lại).
    pub sample_size: usize,
    /// Kích thước shingle: số điểm liên tiếp ghép thành 1 vector đầu vào.
    pub shingle_size: usize,
    /// Số điểm tối đa một lá có thể giữ trước khi cố tách lá.
    /// Lá không tách được (toàn điểm trùng) sẽ tích lũy — chấp nhận được.
    pub leaf_capacity: usize,
}

impl Default for RcfConfig {
    fn default() -> Self {
        Self {
            num_trees: 50,
            sample_size: 256,
            shingle_size: 1,
            leaf_capacity: 1,
        }
    }
}

/// Rừng RCF streaming trên vector `dims` chiều.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RcfForest {
    dims: usize,
    cfg: RcfConfig,
    rng: XorShift64,
    trees: Vec<Node>,
    /// Điểm đã chèn (điểm shingled), phục vụ FIFO eviction.
    window: VecDeque<(u64, Vec<f64>)>,
    /// Buffer trượt cho shingling.
    shingle: VecDeque<Vec<f64>>,
    next_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct XorShift64(u64);

impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// uniform trong [0, 1)
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// uniform trong [lo, hi)
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + self.next_f64() * (hi - lo)
    }
}

/// Bounding box của một subtree.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BBox {
    min: Vec<f64>,
    max: Vec<f64>,
}

impl BBox {
    fn from_point(p: &[f64]) -> Self {
        Self {
            min: p.to_vec(),
            max: p.to_vec(),
        }
    }

    fn update(&mut self, p: &[f64]) {
        for (i, &v) in p.iter().enumerate() {
            if v < self.min[i] {
                self.min[i] = v;
            }
            if v > self.max[i] {
                self.max[i] = v;
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Node {
    Empty,
    Branch {
        left: Box<Node>,
        right: Box<Node>,
        cut_dim: usize,
        cut: f64,
        bbox: BBox,
        size: usize,
    },
    /// Lá chứa 1..n điểm; thông thường đúng 1, nhiều hơn khi toàn điểm trùng
    /// (random cut không tách được).
    Leaf {
        entries: Vec<(u64, Vec<f64>)>,
    },
}

impl Node {
    fn leaf(id: u64, point: Vec<f64>) -> Self {
        Node::Leaf {
            entries: vec![(id, point)],
        }
    }

    fn size(&self) -> usize {
        match self {
            Node::Empty => 0,
            Node::Branch { size, .. } => *size,
            Node::Leaf { entries } => entries.len(),
        }
    }

    /// Chèn điểm, trả về các kích thước sibling trên đường đi (từ lá lên gốc)
    /// phục vụ chấm điểm collusive displacement.
    fn insert(&mut self, id: u64, point: &[f64], rng: &mut XorShift64, leaf_capacity: usize) -> Vec<usize> {
        let mut siblings = Vec::new();
        self.insert_inner(id, point, rng, leaf_capacity, &mut siblings);
        siblings.reverse();
        siblings
    }

    fn insert_inner(
        &mut self,
        id: u64,
        point: &[f64],
        rng: &mut XorShift64,
        leaf_capacity: usize,
        siblings: &mut Vec<usize>,
    ) {
        match self {
            Node::Empty => {
                *self = Node::leaf(id, point.to_vec());
            }
            Node::Leaf { entries } => {
                if entries.len() < leaf_capacity {
                    entries.push((id, point.to_vec()));
                    return;
                }
                // Lá đầy: thử tách thành branch bằng random cut.
                if let Some((cut_dim, cut)) = split_cut(entries.iter().map(|(_, p)| p.as_slice()), rng) {
                    let mut left_entries = Vec::new();
                    let mut right_entries = Vec::new();
                    for (eid, ep) in entries.drain(..) {
                        if ep[cut_dim] < cut {
                            left_entries.push((eid, ep));
                        } else {
                            right_entries.push((eid, ep));
                        }
                    }
                    // Điểm mới về phía đúng theo cut.
                    if point[cut_dim] < cut {
                        left_entries.push((id, point.to_vec()));
                    } else {
                        right_entries.push((id, point.to_vec()));
                    }
                    let mut bbox = BBox::from_point(point);
                    for (_, ep) in left_entries.iter().chain(&right_entries) {
                        bbox.update(ep);
                    }
                    let size = left_entries.len() + right_entries.len();
                    *self = Node::Branch {
                        left: Box::new(Node::Leaf {
                            entries: left_entries,
                        }),
                        right: Box::new(Node::Leaf {
                            entries: right_entries,
                        }),
                        cut_dim,
                        cut,
                        bbox,
                        size,
                    };
                    // Điểm mới nằm trong một lá con; sibling là lá còn lại.
                    let (goes_left, sib) = match self {
                        Node::Branch { left, right, .. } => {
                            let sib_size = if left.leaf_contains(id) { right.size() } else { left.size() };
                            (left.leaf_contains(id), sib_size)
                        }
                        _ => unreachable!(),
                    };
                    siblings.push(sib);
                    let target = if goes_left {
                        match self {
                            Node::Branch { left, .. } => left,
                            _ => unreachable!(),
                        }
                    } else {
                        match self {
                            Node::Branch { right, .. } => right,
                            _ => unreachable!(),
                        }
                    };
                    target.insert_inner(id, point, rng, leaf_capacity, siblings);
                } else {
                    // Toàn điểm trùng: tích lũy trong lá.
                    entries.push((id, point.to_vec()));
                }
            }
            Node::Branch {
                left,
                right,
                cut_dim,
                cut,
                bbox,
                size,
            } => {
                bbox.update(point);
                *size += 1;
                let goes_left = point[*cut_dim] < *cut;
                siblings.push(if goes_left { right.size() } else { left.size() });
                let target: &mut Node = if goes_left { left } else { right };
                target.insert_inner(id, point, rng, leaf_capacity, siblings);
            }
        }
    }

    fn leaf_contains(&self, id: u64) -> bool {
        match self {
            Node::Leaf { entries } => entries.iter().any(|(eid, _)| *eid == id),
            _ => false,
        }
    }

    /// Xoá điểm theo id. Sau xoá, node rỗng trở thành `Empty`; branch chỉ còn
    /// một con bị thay bằng con đó. Trả về true nếu tìm thấy và xoá.
    fn remove(&mut self, id: u64) -> bool {
        match self {
            Node::Empty => false,
            Node::Leaf { entries } => {
                let before = entries.len();
                entries.retain(|(eid, _)| *eid != id);
                if entries.len() < before {
                    if entries.is_empty() {
                        *self = Node::Empty;
                    }
                    true
                } else {
                    false
                }
            }
            Node::Branch {
                left,
                right,
                bbox: _,
                size,
                ..
            } => {
                let removed = if left.remove(id) {
                    true
                } else {
                    right.remove(id)
                };
                if !removed {
                    return false;
                }
                *size -= 1;
                if matches!(**left, Node::Empty) {
                    let right = std::mem::replace(right, Box::new(Node::Empty));
                    *self = *right;
                } else if matches!(**right, Node::Empty) {
                    let left = std::mem::replace(left, Box::new(Node::Empty));
                    *self = *left;
                }
                true
            }
        }
    }
}

/// Chọn (dim, cut) sao cho cut nằm chắc trong (min, max) của dim đó.
fn split_cut<'a, I>(points: I, rng: &mut XorShift64) -> Option<(usize, f64)>
where
    I: IntoIterator<Item = &'a [f64]>,
{
    let mut min: Option<Vec<f64>> = None;
    let mut max: Option<Vec<f64>> = None;
    for p in points {
        match (&mut min, &mut max) {
            (None, None) => {
                min = Some(p.to_vec());
                max = Some(p.to_vec());
            }
            (Some(lo), Some(hi)) => {
                for (i, &v) in p.iter().enumerate() {
                    if v < lo[i] {
                        lo[i] = v;
                    }
                    if v > hi[i] {
                        hi[i] = v;
                    }
                }
            }
            _ => unreachable!(),
        }
    }
    let (lo, hi) = (min?, max?);

    // Chọn dim theo tỉ lệ extent (tối đa 8 lần thử để tránh dim extent = 0).
    let extents: Vec<f64> = lo.iter().zip(&hi).map(|(&a, &b)| (b - a).max(0.0)).collect();
    let total: f64 = extents.iter().sum();
    if total <= 0.0 {
        return None;
    }
    for _ in 0..8 {
        let mut pick = rng.next_f64() * total;
        let mut dim = extents.len() - 1;
        for (i, &e) in extents.iter().enumerate() {
            if pick < e {
                dim = i;
                break;
            }
            pick -= e;
        }
        if extents[dim] > 0.0 {
            let cut = rng.range(lo[dim], hi[dim]);
            return Some((dim, cut));
        }
    }
    None
}

impl RcfForest {
    /// Rừng với cấu hình mặc định (50 cây, mẫu 256, shingle 1).
    #[must_use]
    pub fn new(dims: usize) -> Self {
        Self::with_config(RcfConfig::default(), dims)
    }

    /// Rừng với cấu hình tuỳ chọn.
    #[must_use]
    pub fn with_config(cfg: RcfConfig, dims: usize) -> Self {
        assert!(dims >= 1, "RCF cần tối thiểu 1 chiều");
        assert!(cfg.num_trees >= 1, "num_trees >= 1");
        assert!(cfg.sample_size >= 2, "sample_size >= 2");
        assert!(cfg.shingle_size >= 1, "shingle_size >= 1");
        Self {
            dims,
            rng: XorShift64(0x9E37_79B9_7F4A_7C15),
            trees: vec![Node::Empty; cfg.num_trees],
            cfg,
            window: VecDeque::new(),
            shingle: VecDeque::with_capacity(cfg.shingle_size),
            next_id: 1,
        }
    }

    #[must_use]
    pub fn dims(&self) -> usize {
        self.dims
    }

    #[must_use]
    pub fn config(&self) -> &RcfConfig {
        &self.cfg
    }

    /// Số điểm hiện có trong cửa sổ trượt.
    #[must_use]
    pub fn len(&self) -> usize {
        self.window.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.window.is_empty()
    }

    /// Nạp một điểm (đã đủ `dims` chiều) vào rừng và trả về điểm bất thường
    /// của chính điểm đó (collusive displacement trung bình trên rừng).
    ///
    /// Nếu `shingle_size > 1`, buffer nội bộ ghép `shingle_size` điểm liên
    /// tiếp thành một vector; trả `None` cho tới khi buffer đầy.
    ///
    /// # Errors
    /// Trả `Err` nếu số chiều của điểm không khớp `dims`.
    pub fn add(&mut self, point: &[f64]) -> Result<Option<f64>, String> {
        if point.len() != self.dims {
            return Err(format!(
                "RCF cần vector {} chiều, nhận được {} chiều",
                self.dims,
                point.len()
            ));
        }
        if self.cfg.shingle_size > 1 {
            self.shingle.push_back(point.to_vec());
            if self.shingle.len() < self.cfg.shingle_size {
                return Ok(None);
            }
            let shingled: Vec<f64> = self
                .shingle
                .iter()
                .flat_map(|p| p.iter().copied())
                .collect();
            self.shingle.pop_front();
            let score = self.insert_shingled(&shingled);
            return Ok(Some(score));
        }
        let score = self.insert_shingled(point);
        Ok(Some(score))
    }

    /// Chấm điểm một điểm mà **không** đưa vào mô hình (transient scoring).
    ///
    /// # Errors
    /// Trả `Err` nếu số chiều không khớp.
    pub fn score(&mut self, point: &[f64]) -> Result<f64, String> {
        if point.len() != self.dims {
            return Err(format!(
                "RCF cần vector {} chiều, nhận được {} chiều",
                self.dims,
                point.len()
            ));
        }
        let shingled: Vec<f64> = if self.cfg.shingle_size > 1 {
            let mut tail: Vec<&[f64]> = self.shingle.iter().map(|p| p.as_slice()).collect();
            tail.push(point);
            let full: Vec<f64> = tail.concat();
            if full.len() < self.dims * self.cfg.shingle_size {
                // Chưa đủ shingle: chấm trên những gì có (coi như shingle ngắn).
                full
            } else {
                full[full.len() - self.dims * self.cfg.shingle_size..].to_vec()
            }
        } else {
            point.to_vec()
        };
        let id = self.next_id;
        self.next_id += 1;
        let mut total = 0.0;
        for tree in &mut self.trees {
            let siblings = tree.insert(id, &shingled, &mut self.rng, self.cfg.leaf_capacity);
            total += codisp(&siblings);
            tree.remove(id);
        }
        Ok(total / self.cfg.num_trees as f64)
    }

    fn insert_shingled(&mut self, shingled: &[f64]) -> f64 {
        let id = self.next_id;
        self.next_id += 1;
        let mut total = 0.0;
        for tree in &mut self.trees {
            let siblings = tree.insert(id, shingled, &mut self.rng, self.cfg.leaf_capacity);
            total += codisp(&siblings);
        }
        self.window.push_back((id, shingled.to_vec()));
        if self.window.len() > self.cfg.sample_size {
            let (old_id, _) = self.window.pop_front().expect("window non-empty");
            for tree in &mut self.trees {
                tree.remove(old_id);
            }
        }
        total / self.cfg.num_trees as f64
    }
}

/// Collusive displacement từ danh sách kích thước sibling (từ lá lên gốc,
/// đã reverse). Với mỗi ancestor chứa kích thước phía điểm `c` và sibling `s`:
/// `log(c + s) / log(min(c, s) + 1)`, lấy max. Điểm cô lập → sibling lớn trong
/// khi phía điểm chỉ 1 → `log(n)/log(2)` lớn; điểm thường → các cặp cân bằng → ~1.
fn codisp(siblings: &[usize]) -> f64 {
    let ln = f64::ln;
    // Đi từ lá lên gốc: phía chứa điểm có kích thước tăng dần.
    let mut containing = 1.0; // chính lá/điểm
    let mut best = 1.0;
    for &s in siblings {
        let s = s as f64;
        if s <= 0.0 {
            containing += 1.0;
            continue;
        }
        let cand = ln(containing + s) / ln(containing.min(s) + 1.0);
        if cand > best {
            best = cand;
        }
        containing += s;
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn smooth_series(n: usize) -> Vec<f64> {
        (0..n).map(|i| 50.0 + 10.0 * (i as f64 * 0.3).sin()).collect()
    }

    #[test]
    fn outlier_scores_higher_than_smooth_points() {
        let mut forest = RcfForest::new(1);
        for v in smooth_series(300) {
            let _ = forest.add(&[v]).unwrap();
        }
        let normal = forest.score(&[50.0]).unwrap();
        let spike = forest.score(&[500.0]).unwrap();
        assert!(
            spike > normal * 3.0,
            "spike {spike} phải cao hơn nhiều so với normal {normal}"
        );
        assert!(spike > 2.0, "spike {spike} phải vượt ngưỡng bất thường");
    }

    #[test]
    fn streaming_add_flags_spike() {
        let mut forest = RcfForest::new(1);
        let mut scores = Vec::new();
        for v in smooth_series(200) {
            scores.push(forest.add(&[v]).unwrap().unwrap());
        }
        let spike_score = forest.add(&[500.0]).unwrap().unwrap();
        let base: f64 = scores.iter().sum::<f64>() / scores.len() as f64;
        assert!(spike_score > base * 2.0, "spike {spike_score} vs base {base}");
    }

    #[test]
    fn shingling_detects_shape_change() {
        // Chuỗi phẳng rồi suddenly nhảy biên độ dao động.
        let mut forest = RcfForest::with_config(
            RcfConfig {
                shingle_size: 8,
                ..RcfConfig::default()
            },
            1,
        );
        let mut last = 0.0;
        for i in 0..400 {
            let v = if i < 200 { 0.0 } else { (i as f64 * 0.9).sin() * 40.0 };
            if let Some(s) = forest.add(&[v]).unwrap() {
                last = s;
            }
            let _ = last;
        }
        // Điểm lệch hình dạng (đảo dấu đột ngột trong shingle).
        let odd = forest.score(&[-40.0]).unwrap();
        let normal = forest.score(&[0.0]).unwrap();
        assert!(odd > normal, "shingle lệch {odd} phải cao hơn thường {normal}");
    }

    #[test]
    fn eviction_keeps_window_bounded() {
        let mut forest = RcfForest::with_config(
            RcfConfig {
                num_trees: 5,
                sample_size: 32,
                ..RcfConfig::default()
            },
            2,
        );
        for i in 0..500 {
            forest.add(&[i as f64 % 100.0, (i as f64).sqrt()]).unwrap();
        }
        assert_eq!(forest.len(), 32);
    }

    #[test]
    fn multivariate_outlier_detected() {
        let mut forest = RcfForest::new(2);
        // Cụm quanh (10, 10) với nhiễu nhỏ.
        for i in 0..300 {
            let jitter = (i as f64 * 0.7).sin() * 0.5;
            forest.add(&[10.0 + jitter, 10.0 - jitter]).unwrap();
        }
        let normal = forest.score(&[10.2, 9.8]).unwrap();
        let outlier = forest.score(&[80.0, 5.0]).unwrap();
        assert!(outlier > normal * 2.0, "{outlier} vs {normal}");
    }

    #[test]
    fn transient_score_does_not_change_model() {
        let mut forest = RcfForest::new(1);
        for v in smooth_series(200) {
            forest.add(&[v]).unwrap();
        }
        let before = forest.len();
        let s1 = forest.score(&[50.0]).unwrap();
        assert_eq!(forest.len(), before);
        let s2 = forest.score(&[50.0]).unwrap();
        assert!((s1 - s2).abs() < 1e-12, "score phải deterministic: {s1} vs {s2}");
    }
}
