//! Aho-Corasick multi-pattern substring matcher.
//!
//! Fully in-memory (không dùng storage backend): automaton (transitions,
//! failure, output, root inputs) nằm trực tiếp trong cấu trúc `AhoCorasick`.
//! Public API giữ nguyên: `new`, `new_with_callbacks`, `add`, `optimize`
//! (async), `similar` (async).

use std::collections::{BTreeMap, VecDeque};

type MappingBox = Box<dyn Fn(&String, &BTreeMap<String, usize>) -> Option<usize> + Send + Sync>;
type CompareBox = Box<dyn Fn(&String, &String) -> bool + Send + Sync>;
type CollectBox = Box<dyn Fn(&String) + Send + Sync>;
type SplitBox = Box<dyn Fn(&String) -> Vec<String> + Send + Sync>;

type MappingFn =
    &'static (dyn Fn(&String, &BTreeMap<String, usize>) -> Option<usize> + Send + Sync);
type CompareFn = &'static (dyn Fn(&String, &String) -> bool + Send + Sync);
type CollectFn = &'static (dyn Fn(&String) + Send + Sync);
type SplitFn = &'static (dyn Fn(&String) -> Vec<String> + Send + Sync);

pub struct AhoCorasick {
    // @NOTE: callbacks
    mapping_fn: MappingBox,
    compare_fn: CompareBox,
    collect_fn: CollectBox,
    split_fn: SplitBox,

    // @NOTE: pattern registry (pre-optimization)
    pattern_mapping: BTreeMap<String, usize>,
    patterns: Vec<String>,

    // @NOTE: automaton (in-memory — indexing giống storage cũ)
    // State 0 = root.
    labels: Vec<String>,
    next: Vec<BTreeMap<String, usize>>,
    back: Vec<usize>,
    failure: Vec<usize>,
    output: Vec<Option<usize>>,
    root_inputs: Vec<usize>,

    // @NOTE: flags
    is_optimized: bool,
}

impl Default for AhoCorasick {
    fn default() -> Self {
        Self::new()
    }
}

impl AhoCorasick {
    pub fn new() -> Self {
        Self::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        )
    }

    pub fn new_with_callbacks(
        mapping_fn: MappingFn,
        compare_fn: CompareFn,
        collect_fn: CollectFn,
        split_fn: SplitFn,
    ) -> Self {
        Self {
            mapping_fn: Box::new(mapping_fn),
            compare_fn: Box::new(compare_fn),
            collect_fn: Box::new(collect_fn),
            split_fn: Box::new(split_fn),

            pattern_mapping: BTreeMap::new(),
            patterns: Vec::new(),

            // Root state (id 0).
            labels: vec![String::new()],
            next: vec![BTreeMap::new()],
            back: vec![0],
            failure: vec![0],
            output: vec![None],
            root_inputs: Vec::new(),

            is_optimized: false,
        }
    }

    pub fn add(&mut self, pattern: String) {
        if !pattern.is_empty() && !self.pattern_mapping.contains_key(&pattern) {
            // @NOTE: configure new state machine
            self.pattern_mapping
                .insert(pattern.clone(), self.pattern_mapping.len());

            // @NOTE: add context
            self.patterns.push(pattern.clone());

            // @NOTE: reset optimized flag
            self.is_optimized = false;
        }
    }

    /// Số pattern đã đăng ký (chưa tính automaton states).
    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Danh sách pattern strings đã đăng ký.
    #[must_use]
    pub fn patterns(&self) -> Vec<String> {
        self.patterns.clone()
    }

    /// Kiểm tra automaton đã được optimize chưa.
    #[must_use]
    pub fn is_optimized(&self) -> bool {
        self.is_optimized
    }

    pub async fn optimize(&mut self) {
        // @NOTE: rebuild automaton from patterns (idempotent re-optimize).
        self.labels = vec![String::new()];
        self.next = vec![BTreeMap::new()];
        self.back = vec![0];
        self.failure = vec![0];
        self.output = vec![None];
        self.root_inputs = Vec::new();

        let mut queue = VecDeque::<usize>::new();
        let mut state: usize = 1;

        for i in 0..self.patterns.len() {
            let pattern = &self.patterns[i];
            let mut current_state = 0_usize;
            let mut next_state = current_state;

            for block in (self.split_fn)(pattern) {
                let possible_next_state = self.next[current_state].get(&block);

                if let Some(possible_next_state) = possible_next_state {
                    next_state = *possible_next_state;
                } else {
                    // @NOTE: if next state not found, build it
                    let index = self.labels.len();
                    let next_block = block.clone();

                    // @NOTE: record transition (kể cả từ root — root children
                    // phải nằm trong `next[0]` để failure mapping + dedup hoạt động;
                    // `root_inputs` giữ thêm danh sách distinct root children).
                    self.next[current_state].insert(next_block.clone(), index);

                    self.labels.push(block.clone());
                    self.next.push(BTreeMap::new());
                    self.back.push(current_state);
                    self.failure.push(0);
                    self.output.push(None);

                    if current_state == 0 {
                        // @NOTE: this is the open state, new flow has been created
                        self.root_inputs.push(index);
                    }

                    next_state = state;
                    state += 1;
                }

                current_state = next_state;
            }

            // @NOTE: we go to the end of the pattern, mark this as output
            self.output[next_state] = Some(i);
        }

        // @NOTE: build failure mapping
        queue.push_back(0);

        while !queue.is_empty() {
            let i = queue.pop_front().unwrap();
            let label = &self.labels[i];
            let mut failure_of_previous = self.failure[self.back[i]];
            let mut break_at_last = false;

            if self.back[i] != 0 {
                loop {
                    let failure_state = self.next[failure_of_previous]
                        .iter()
                        .find(|(l, _)| *l == label)
                        .map(|(_, s)| *s);

                    match failure_state {
                        Some(failure_state) => {
                            self.failure[i] = failure_state;
                            break;
                        }
                        None => {
                            if break_at_last {
                                break;
                            }

                            let try_failure = self.failure[failure_of_previous];

                            if try_failure == failure_of_previous {
                                break_at_last = true;
                            }

                            failure_of_previous = try_failure;
                        }
                    }
                }
            }

            for next_state in self.next[i].values() {
                queue.push_back(*next_state);
            }
        }

        self.is_optimized = true;
    }

    pub async fn similar(&self, sample: &String) -> bool {
        let blocks = (self.split_fn)(sample);
        let mut state = 0_usize;
        let mut i = 0_usize;

        if !self.is_optimized {
            return false;
        }

        while i < blocks.len() {
            let mut next_state = 0_usize;
            let block = &blocks[i];

            if state == 0 {
                // @NOTE: first state, find matching initial string
                for first_id in &self.root_inputs {
                    if (self.compare_fn)(block, &self.labels[*first_id]) {
                        state = *first_id;
                        break;
                    }
                }

                // @NOTE: skip transition block – root input resolved above.
            } else {
                // @NOTE: move to next state from current state
                let mapping: BTreeMap<String, usize> = self.next[state].clone();

                match (self.mapping_fn)(block, &mapping) {
                    Some(possible_next_state) => {
                        next_state = possible_next_state;
                    }
                    None => {
                        for (template, possible_next_state) in &mapping {
                            if (self.compare_fn)(block, template) {
                                next_state = *possible_next_state;
                                break;
                            }
                        }
                    }
                }

                if next_state != 0 {
                    // @NOTE: collect variables for this possible flow
                    (self.collect_fn)(block);
                }

                if next_state == 0 {
                    // @NOTE: not found the next state, use failure mapping
                    state = self.failure[state];
                    continue;
                } else {
                    state = next_state;
                }
            }

            // @NOTE: output có thể nằm trên failure chain (các pattern là hậu tố
            // của pattern đang match) — dò xuống chain cho tới root.
            let mut probe = state;
            while probe != 0 {
                if self.output[probe].is_some() {
                    return true;
                }
                probe = self.failure[probe];
            }

            i += 1;
        }

        false
    }
}

// ==================== PatternStorage ====================
// NOTE: `PatternStorage` trait được định nghĩa tại `crate::storage` và được
// implement bởi mọi backend (InMemoryStorage, SqliteStorage, RedisStorage).
// Trait này không còn nằm trong file này.

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_ahocorasick() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("he".to_string());
        ahocorasick.add("she".to_string());
        ahocorasick.add("his".to_string());
        ahocorasick.add("hers".to_string());
        ahocorasick.optimize().await;

        assert!(!ahocorasick.similar(&"us".to_string()).await);
        assert!(!ahocorasick.similar(&"x".to_string()).await);

        assert!(ahocorasick.similar(&"she".to_string()).await);
        assert!(ahocorasick.similar(&"he".to_string()).await);
        assert!(ahocorasick.similar(&"his".to_string()).await);
        assert!(ahocorasick.similar(&"hers".to_string()).await);
        assert!(ahocorasick.similar(&"hello".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_complex_pattern() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("CG".to_string());
        ahocorasick.add("TGC".to_string());
        ahocorasick.add("CGT".to_string());
        ahocorasick.add("GCC".to_string());
        ahocorasick.add("GTGC".to_string());
        ahocorasick.add("TCGT".to_string());
        ahocorasick.optimize().await;

        assert!(ahocorasick.similar(&"CG".to_string()).await);
        assert!(ahocorasick.similar(&"TGC".to_string()).await);
        assert!(ahocorasick.similar(&"CGT".to_string()).await);
        assert!(ahocorasick.similar(&"GCC".to_string()).await);
        assert!(ahocorasick.similar(&"GTGC".to_string()).await);
        assert!(ahocorasick.similar(&"TCGT".to_string()).await);

        assert!(ahocorasick.similar(&"ACGT".to_string()).await);
        assert!(ahocorasick.similar(&"GTCG".to_string()).await);
        assert!(ahocorasick.similar(&"TACG".to_string()).await);

        assert!(!ahocorasick.similar(&"AAA".to_string()).await);
        assert!(!ahocorasick.similar(&"GGG".to_string()).await);
        assert!(!ahocorasick.similar(&"CCC".to_string()).await);
        assert!(!ahocorasick.similar(&"TTT".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_vietnammese() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("ư".to_string());
        ahocorasick.add("ới".to_string());
        ahocorasick.optimize().await;

        assert!(ahocorasick.similar(&("ư".to_string())).await);
        assert!(!ahocorasick.similar(&("ơi".to_string())).await);
        assert!(ahocorasick.similar(&("ưới".to_string())).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_empty_pattern() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add(String::from(""));
        ahocorasick.optimize().await;

        assert!(!ahocorasick.similar(&"something".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_duplicate_pattern() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("he".to_string());
        ahocorasick.add("he".to_string());
        ahocorasick.add("she".to_string());
        ahocorasick.optimize().await;

        assert!(!ahocorasick.similar(&"us".to_string()).await);
        assert!(!ahocorasick.similar(&"x".to_string()).await);

        assert!(ahocorasick.similar(&"she".to_string()).await);
        assert!(ahocorasick.similar(&"he".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_special_characters() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("h,e".to_string());
        ahocorasick.add("s h e".to_string());
        ahocorasick.optimize().await;

        assert!(ahocorasick.similar(&"h,e".to_string()).await);
        assert!(ahocorasick.similar(&"s h e".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_no_pattern() {
        let ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        assert!(!ahocorasick.similar(&"something".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_with_partial_match() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("abc".to_string());
        ahocorasick.add("def".to_string());
        ahocorasick.optimize().await;

        assert!(!ahocorasick.similar(&"ab".to_string()).await);
        assert!(ahocorasick.similar(&"abc".to_string()).await);
        // @NOTE: Aho-Corasick là substring matching, "abcdef" chứa "abc" → match
        assert!(ahocorasick.similar(&"abcdef".to_string()).await);
        assert!(ahocorasick.similar(&"def".to_string()).await);
        assert!(!ahocorasick.similar(&"de".to_string()).await);
    }

    #[tokio::test]
    async fn test_ahocorasick_performance() {
        let words = vec![
            "informatics",
            "information",
            "informative",
            "informing",
            "informant",
            "informally",
            "informal",
            "informed",
            "informer",
            "informers",
            "informing",
            "inform",
            "info",
            "infographic",
            "infographics",
            "infomercial",
            "infomercials",
            "infotainment",
            "infotainments",
            "infomania",
        ];

        let mut tries = AhoCorasick::new();
        for word in &words {
            tries.add(word.to_string());
        }
        tries.optimize().await;

        let inputs = vec![
            "informatics",
            "information",
            "informative",
            "informing",
            "t",
            "a",
            "b",
            "c",
            "infotainments",
            "infomania",
            "unknown",
            "nothing",
        ];

        let mut outputs = vec![false; inputs.len()];
        let now = Instant::now();
        // sequential
        for i in 0..inputs.len() {
            outputs[i] = tries.similar(&inputs[i].to_string()).await;
        }

        let elapsed = now.elapsed();
        println!(
            "\n\nAhoCorasick – single thread: {}ms\n\n",
            elapsed.as_millis()
        );

        assert!(outputs[0]); // informatics
        assert!(outputs[1]); // information
        assert!(outputs[2]); // informative
        assert!(outputs[3]); // informing
        assert!(!outputs[4]); // t (not a keyword)
        assert!(!outputs[5]); // a
        assert!(!outputs[6]); // b
        assert!(!outputs[7]); // c
        assert!(outputs[8]); // infotainments
        assert!(outputs[9]); // infomania
        assert!(!outputs[10]); // unknown
        assert!(!outputs[11]); // nothing
    }

    #[tokio::test]
    async fn test_parallel_searching() {
        let mut ahocorasick = AhoCorasick::new_with_callbacks(
            &|block: &String, mapping: &BTreeMap<String, usize>| -> Option<usize> {
                mapping.get(block).cloned()
            },
            &|left: &String, right: &String| -> bool { left == right },
            &|_block: &String| {},
            &|pattern: &String| -> Vec<String> {
                pattern
                    .split("")
                    .filter(|block| !block.is_empty())
                    .map(|block| block.to_string())
                    .collect()
            },
        );

        ahocorasick.add("he".to_string());
        ahocorasick.add("she".to_string());
        ahocorasick.add("his".to_string());
        ahocorasick.add("hers".to_string());
        ahocorasick.optimize().await;

        let samples: Vec<String> = vec![
            "us".to_string(),
            "she".to_string(),
            "he".to_string(),
            "his".to_string(),
            "hers".to_string(),
            "hello".to_string(),
            "x".to_string(),
        ];

        let now = Instant::now();
        // sequential (similar is async)
        let mut results = vec![false; samples.len()];
        for i in 0..samples.len() {
            results[i] = ahocorasick.similar(&samples[i]).await;
        }

        let elapsed = now.elapsed();
        println!(
            "\n\nAhoCorasick – sequential: {:?}ms\n\n",
            elapsed.as_millis()
        );

        assert!(!results[0]); // us
        assert!(results[1]); // she
        assert!(results[2]); // he
        assert!(results[3]); // his
        assert!(results[4]); // hers
        assert!(results[5]); // hello
        assert!(!results[6]); // x
    }
}
