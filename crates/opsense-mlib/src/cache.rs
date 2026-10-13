//! Redis cache backend implementation for opsense-mlib.
//!
//! This module provides the `RedisBackend` which implements the
//! `opsense_ffi::cache::CacheBackend` trait.

#[cfg(feature = "redis")]
use opsense_ffi::cache::{CacheBackend, CacheError};

#[cfg(feature = "redis")]
pub mod redis_backend {
    use super::*;
    use redis::AsyncCommands;
    use redis::aio::MultiplexedConnection;

    pub struct RedisBackend(pub MultiplexedConnection);

    impl RedisBackend {
        #[allow(clippy::new_ret_no_self)]
        pub fn new(conn: MultiplexedConnection) -> std::sync::Arc<dyn CacheBackend> {
            std::sync::Arc::new(Self(conn))
        }
    }

    impl CacheBackend for RedisBackend {
        fn get(
            &self,
            key: &str,
        ) -> opsense_ffi::cache::BoxFuture<Result<Option<String>, CacheError>> {
            let key = key.to_string();
            let mut conn = self.0.clone();
            Box::pin(async move {
                conn.get::<_, Option<String>>(&key)
                    .await
                    .map_err(|e| CacheError::Backend(e.to_string()))
            })
        }

        fn set_ex(
            &self,
            key: &str,
            value: &str,
            ttl: u64,
        ) -> opsense_ffi::cache::BoxFuture<Result<(), CacheError>> {
            let key = key.to_string();
            let value = value.to_string();
            let mut conn = self.0.clone();
            Box::pin(async move {
                conn.set_ex::<_, _, ()>(&key, &value, ttl)
                    .await
                    .map_err(|e| CacheError::Backend(e.to_string()))
            })
        }

        fn del(&self, key: &str) -> opsense_ffi::cache::BoxFuture<Result<(), CacheError>> {
            let key = key.to_string();
            let mut conn = self.0.clone();
            Box::pin(async move {
                conn.del::<_, ()>(&key)
                    .await
                    .map_err(|e| CacheError::Backend(e.to_string()))
            })
        }

        fn exists(&self, key: &str) -> opsense_ffi::cache::BoxFuture<Result<bool, CacheError>> {
            let key = key.to_string();
            let mut conn = self.0.clone();
            Box::pin(async move {
                conn.exists(&key)
                    .await
                    .map_err(|e| CacheError::Backend(e.to_string()))
            })
        }
    }
}

#[cfg(feature = "redis")]
pub use redis_backend::RedisBackend;

#[cfg(all(test, feature = "redis"))]
mod tests {
    use opsense_ffi::cache::CacheError;

    #[test]
    fn cache_error_miss_distinct_from_backend() {
        assert_ne!(
            std::mem::discriminant(&CacheError::Miss),
            std::mem::discriminant(&CacheError::Backend(String::new()))
        );
    }
}
