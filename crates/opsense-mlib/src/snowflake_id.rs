use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SnowflakeId {
    start_time: u64,
    machine_id: u16,
    state: AtomicU64,
}

impl SnowflakeId {
    pub fn new(machine_id: u16, start_time: u64) -> Self {
        if machine_id >= 1 << 10 {
            panic!("machine_id quá lớn, chỉ dùng 0..1023");
        }

        Self {
            start_time,
            machine_id,
            state: AtomicU64::new(0),
        }
    }

    pub fn get_machine_id(&self) -> u16 {
        self.machine_id
    }

    pub fn get_start_time(&self) -> u64 {
        self.start_time
    }

    pub fn generate(&self) -> i64 {
        loop {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Clock moved backwards")
                .as_millis() as u64;

            let current_ts = now_ms.saturating_sub(self.start_time);
            let old_state = self.state.load(Ordering::Acquire);

            let old_ts = old_state >> 12;
            let old_seq = old_state & 0xFFF;

            let (new_ts, new_seq) = if current_ts > old_ts {
                (current_ts, 0)
            } else {
                let next_seq = (old_seq + 1) & 0xFFF;
                if next_seq == 0 {
                    continue;
                }
                (old_ts, next_seq)
            };

            let new_state = (new_ts << 12) | new_seq;

            if self
                .state
                .compare_exchange_weak(old_state, new_state, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let id = ((new_ts & 0xFFFFFFFFFF) << 22)
                    | ((new_seq & 0xFFF) << 10)
                    | (self.machine_id as u64 & 0x3FF);

                return id as i64;
            }
        }
    }
}

impl Default for SnowflakeId {
    fn default() -> Self {
        let machine_id = std::env::var("OPSENSE_MACHINE_ID")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        Self::new(machine_id, start_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    fn new_snowflake() -> SnowflakeId {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 1000;

        SnowflakeId::new(42, start_time)
    }

    #[test]
    fn test_generate_does_not_panic() {
        let generator = new_snowflake();
        let id = generator.generate();
        assert!(id > 0, "ID must be positive");
    }

    #[test]
    fn test_ids_are_unique_in_single_thread_burst() {
        let generator = new_snowflake();
        let mut ids = HashSet::new();
        let count = 10_000;

        for _ in 0..count {
            let id = generator.generate();
            assert!(ids.insert(id), "Duplicate ID: {}", id);
        }

        assert_eq!(ids.len(), count, "Có duplicate trong burst 10k ID");
    }

    #[test]
    fn test_sequence_increases_within_same_timestamp() {
        let generator = new_snowflake();

        let mut ids = Vec::new();
        for _ in 0..20 {
            ids.push(generator.generate());
        }

        let unique: HashSet<i64> = ids.iter().cloned().collect();
        assert_eq!(unique.len(), ids.len());

        let seq_bits = ids
            .iter()
            .map(|&id| ((id as u64) >> 10) & 0xFFF)
            .collect::<Vec<u64>>();

        println!("Sequence bits: {:?}", seq_bits); // debug

        for i in 1..seq_bits.len() {
            assert_eq!(
                seq_bits[i],
                seq_bits[i - 1] + 1,
                "Sequence is not incremental: {} → {}",
                seq_bits[i - 1],
                seq_bits[i]
            );
        }
    }

    #[test]
    fn test_timestamp_changes_after_delay() {
        let generator = new_snowflake();

        let id1 = generator.generate();

        thread::sleep(Duration::from_millis(15));

        let id2 = generator.generate();

        let ts1 = (id1 as u64 >> 22) & 0x7FFFFFFFFF;
        let ts2 = (id2 as u64 >> 22) & 0x7FFFFFFFFF;

        assert!(
            ts2 > ts1 || ts2 == ts1 + 1,
            "Timestamp is not change after delay"
        );
    }

    #[test]
    fn test_multi_thread_no_duplicate_under_load() {
        let generator = Arc::new(new_snowflake());
        let threads = 8;
        let per_thread = 50_000;
        let total = threads * per_thread;

        let mut handles = vec![];
        let results = Arc::new(Mutex::new(Vec::new()));

        for _ in 0..threads {
            let gen_clone = generator.clone();
            let results_clone = results.clone();

            handles.push(thread::spawn(move || {
                let mut local_ids = Vec::new();
                for _ in 0..per_thread {
                    local_ids.push(gen_clone.generate());
                }
                let mut res = results_clone.lock().unwrap();
                res.extend(local_ids);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let all_ids = results.lock().unwrap();
        let unique: HashSet<i64> = all_ids.iter().cloned().collect();

        println!(
            "Generated: {} | Unique: {} | Duplicates: {}",
            total,
            unique.len(),
            total - unique.len()
        );

        assert_eq!(
            unique.len(),
            total,
            "Có duplicate khi chạy multi-thread ({} duplicates)",
            total - unique.len()
        );
    }

    #[test]
    #[should_panic(expected = "machine_id quá lớn")]
    fn test_invalid_machine_id_panics() {
        SnowflakeId::new(1 << 10, 0);
    }

    #[test]
    fn test_multi_thread_high_load_no_duplicate() {
        let generator = Arc::new(new_snowflake());
        let threads = 16;
        let per_thread = 50_000;
        let total = threads * per_thread;

        let mut handles = vec![];
        let all_ids = Arc::new(Mutex::new(HashSet::new()));

        for _ in 0..threads {
            let gen_clone = generator.clone();
            let all_ids_clone = all_ids.clone();

            handles.push(thread::spawn(move || {
                let mut local = HashSet::new();
                for _ in 0..per_thread {
                    let id = gen_clone.generate();
                    local.insert(id);
                }
                let mut global = all_ids_clone.lock().unwrap();
                global.extend(local);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let unique_count = all_ids.lock().unwrap().len();
        println!(
            "Generated: {} | Unique: {} | Duplicates: {}",
            total,
            unique_count,
            total - unique_count
        );

        assert_eq!(unique_count, total, "Có duplicate trong multi-thread");
    }
}
