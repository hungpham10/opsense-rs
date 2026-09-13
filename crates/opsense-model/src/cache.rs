use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opsense_libs::binarysearch::{lower_bound, upper_bound};
use redis::{AsyncCommands, ErrorKind, RedisError, RedisResult};

use super::resolver::Resolver;

pub struct Cache {
    resolver: Arc<Resolver>,
    tenant_id: i64,
}

impl Cache {
    pub fn new(resolver: Arc<Resolver>, tenant_id: i64) -> Self {
        Self {
            resolver,
            tenant_id,
        }
    }

    pub async fn get(&self, key: &String) -> RedisResult<String> {
        self.resolver
            .cache(self.tenant_id)
            .get(format!("value[{key}]"))
            .await
    }

    pub async fn set(&self, key: &String, value: &String, ttl: usize) -> RedisResult<()> {
        self.resolver
            .cache(self.tenant_id)
            .set_ex(format!("value[{key}]"), value, ttl as u64)
            .await
    }

    /// Delete a key from the cache.
    pub async fn del(&self, key: &String) -> RedisResult<()> {
        self.resolver
            .cache(self.tenant_id)
            .del::<_, ()>(format!("value[{key}]"))
            .await
    }
}

pub struct Page {
    resolver: Arc<Resolver>,
    tenant_id: i64,
}

impl Page {
    pub fn new(resolver: Arc<Resolver>, tenant_id: i64) -> Self {
        Self {
            resolver,
            tenant_id,
        }
    }

    pub async fn index(&self, key: &String, after: i64, limit: u64) -> RedisResult<u64> {
        let indices = self
            .resolver
            .cache(self.tenant_id)
            .get::<String, Vec<(i64, u64, u64)>>(format!("paging[{key}]:indices"))
            .await?;

        if !indices.is_empty() {
            let comparator =
                |target: &(i64, u64), item: &(i64, u64, u64)| target.cmp(&(item.0, item.1));
            let lb = lower_bound(&indices, &(after, limit), comparator);
            let ub = upper_bound(&indices, &(after, limit), comparator);

            for &(_, size, id) in indices.iter().take(ub).skip(lb) {
                if size >= limit {
                    let key = format!("paging[{key}]:data:{id}");
                    let is_key_alive = self.resolver.cache(self.tenant_id).exists(&key).await?;

                    if is_key_alive {
                        return Ok(id);
                    }
                }
            }
        }

        Err(RedisError::from((ErrorKind::Io, "Page data not found")))
    }

    pub async fn get(&self, key: &String, after: i64, limit: u64) -> RedisResult<String> {
        self.resolver
            .cache(self.tenant_id)
            .get(format!(
                "paging[{key}]:data:{}",
                self.index(key, after, limit).await?
            ))
            .await
    }

    pub async fn set(
        &self,
        key: &String,
        value: &String,
        ttl: usize,
        after: i64,
        limit: u64,
    ) -> RedisResult<()> {
        let mut id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let mut indices = self
            .resolver
            .cache(self.tenant_id)
            .get::<String, Vec<(i64, u64, u64)>>(format!("paging[{key}]:indices"))
            .await
            .unwrap_or_else(|_| vec![]);

        let pos = lower_bound(
            &indices,
            &(after, limit),
            |target: &(i64, u64), item: &(i64, u64, u64)| target.cmp(&(item.0, item.1)),
        );
        if pos < indices.len() && indices[pos].0 == after && indices[pos].1 == limit {
            id = indices[pos].2;
        } else {
            indices.insert(pos, (after, limit, id));

            self.resolver
                .cache(self.tenant_id)
                .set_ex::<_, _, ()>(
                    &format!("paging[{key}]:indices"),
                    serde_json::to_string(&indices).unwrap_or_else(|_| "[]".to_string()),
                    2 * ttl as u64,
                )
                .await?;
        }

        return self
            .resolver
            .cache(self.tenant_id)
            .set_ex::<_, _, ()>(&format!("paging[{key}]:data:{id}"), value, ttl as u64)
            .await;
    }
}
