use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Giống convention đã có ở `mlib/lru.rs:26`.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Binary search utilities — copied from mlib to avoid dependency.
pub fn lower_bound<T, K, F>(arr: &[T], target: &K, comparator: F) -> usize
where
    F: Fn(&K, &T) -> Ordering,
{
    let mut left = 0;
    let mut right = arr.len();

    while left < right {
        let mid = left + (right - left) / 2;

        if comparator(target, &arr[mid]) == Ordering::Greater {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

pub fn upper_bound<T, K, F>(arr: &[T], target: &K, comparator: F) -> usize
where
    F: Fn(&K, &T) -> Ordering,
{
    let mut left = 0;
    let mut right = arr.len();

    while left < right {
        let mid = left + (right - left) / 2;

        if comparator(target, &arr[mid]) == Ordering::Less {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    left
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// Key không có — **phân biệt được** với lỗi backend.
    Miss,
    Backend(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Miss => write!(f, "cache miss"),
            CacheError::Backend(e) => write!(f, "cache backend error: {e}"),
        }
    }
}

impl std::error::Error for CacheError {}

/// Nguồn dữ liệu thật đằng sau `Cache`/`Page`.
///
/// Implementation mặc định là `mlib::cache::RedisBackend` (feature `redis`).
/// Host có thể cung cấp impl bất kỳ nào cũng được, chỉ cần 4 phép này.
///
/// `fn` trả `BoxFuture` chứ không phải `async fn`: không thể `impl Trait`
/// trong trait object, và impl cần clone connection *trước* khi `.await`.
pub trait CacheBackend: Send + Sync + 'static {
    fn get(&self, key: &str) -> BoxFuture<Result<Option<String>, CacheError>>;
    fn set_ex(&self, key: &str, value: &str, ttl: u64) -> BoxFuture<Result<(), CacheError>>;
    fn del(&self, key: &str) -> BoxFuture<Result<(), CacheError>>;
    fn exists(&self, key: &str) -> BoxFuture<Result<bool, CacheError>>;
}

/// Dịch vụ của một tenant, do host dựng **per-request**.
/// Module chỉ nhận — không tự tạo, không đổi được tenant.
#[derive(Clone)]
pub struct TenantServices {
    pub tenant_id: i64,
    pub cache: Option<Arc<dyn CacheBackend>>,
    // mở rộng sau: secret, bucket, db — thêm field vào cuối
}

/// Wrapper trực tiếp trên `CacheBackend` — giữ NGUYÊN format key
/// `value[{key}]`, `paging[{key}]:indices`, `paging[{key}]:data:{id}`
/// và NGUYÊN logic binary search. Bỏ field `tenant_id`.
pub struct Cache {
    backend: Arc<dyn CacheBackend>,
}

impl Cache {
    pub fn new(backend: Arc<dyn CacheBackend>) -> Self {
        Self { backend }
    }

    pub async fn get(&self, key: &String) -> Result<String, CacheError> {
        match self.backend.get(&format!("value[{key}]")).await? {
            Some(v) => Ok(v),
            None => Err(CacheError::Miss),
        }
    }

    pub async fn set(&self, key: &String, value: &String, ttl: usize) -> Result<(), CacheError> {
        self.backend.set_ex(&format!("value[{key}]"), value, ttl as u64).await
    }

    pub async fn del(&self, key: &String) -> Result<(), CacheError> {
        self.backend.del(&format!("value[{key}]")).await
    }
}

/// Page giữ NGUYÊN logic binary search và format key
/// `paging[{key}]:indices` / `paging[{key}]:data:{id}`.
pub struct Page {
    backend: Arc<dyn CacheBackend>,
}

impl Page {
    pub fn new(backend: Arc<dyn CacheBackend>) -> Self {
        Self { backend }
    }

    pub async fn index(&self, key: &String, after: i64, limit: u64) -> Result<u64, CacheError> {
        let indices_str = self
            .backend
            .get(&format!("paging[{key}]:indices"))
            .await?
            .ok_or(CacheError::Miss)?;

        let indices: Vec<(i64, u64, u64)> = serde_json::from_str(&indices_str)
            .ok()
            .unwrap_or_default();

        if !indices.is_empty() {
            let comparator =
                |target: &(i64, u64), item: &(i64, u64, u64)| target.cmp(&(item.0, item.1));
            let lb = lower_bound(&indices, &(after, limit), comparator);
            let ub = upper_bound(&indices, &(after, limit), comparator);

            for &(_, size, id) in indices.iter().take(ub).skip(lb) {
                if size >= limit {
                    let data_key = format!("paging[{key}]:data:{id}");
                    let is_key_alive = self.backend.exists(&data_key).await?;
                    if is_key_alive {
                        return Ok(id);
                    }
                }
            }
        }

        Err(CacheError::Miss)
    }

    pub async fn get(&self, key: &String, after: i64, limit: u64) -> Result<String, CacheError> {
        let id = self.index(key, after, limit).await?;
        self.backend
            .get(&format!("paging[{key}]:data:{id}"))
            .await?
            .ok_or(CacheError::Miss)
    }

    pub async fn set(
        &self,
        key: &String,
        value: &String,
        ttl: usize,
        after: i64,
        limit: u64,
    ) -> Result<(), CacheError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let indices_key = format!("paging[{key}]:indices");
        let indices_str = self.backend.get(&indices_key).await?;
        let mut indices: Vec<(i64, u64, u64)> = indices_str
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let pos = lower_bound(
            &indices,
            &(after, limit),
            |target: &(i64, u64), item: &(i64, u64, u64)| target.cmp(&(item.0, item.1)),
        );
        if pos < indices.len() && indices[pos].0 == after && indices[pos].1 == limit {
            id = indices[pos].2;
        } else {
            indices.insert(pos, (after, limit, id));

            self.backend
                .set_ex(
                    &indices_key,
                    &serde_json::to_string(&indices).unwrap_or_else(|_| "[]".to_string()),
                    2 * ttl as u64,
                )
                .await?;
        }

        self.backend
            .set_ex(&format!("paging[{key}]:data:{id}"), value, ttl as u64)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_error_miss_distinct_from_backend() {
        assert_ne!(
            std::mem::discriminant(&CacheError::Miss),
            std::mem::discriminant(&CacheError::Backend(String::new()))
        );
    }
}