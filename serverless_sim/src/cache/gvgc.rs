use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use crate::fn_dag::FnId;

use super::InstanceCachePolicy;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Generation {
    Young,
    Old,
}

struct CacheEntry {
    gen: Generation,
    hit_count: u64,
    last_touch_tick: u64,
}

struct GlobalHotController {
    alpha: f32,
    cover_threshold: f32,
    period_ops: u64,
    op_in_period: u64,
    period_counts: HashMap<FnId, u64>,
    scores: HashMap<FnId, f32>,
    hot_set: HashSet<FnId>,
}

impl GlobalHotController {
    fn new() -> Self {
        Self {
            alpha: 0.3,
            cover_threshold: 0.8,
            period_ops: 200,
            op_in_period: 0,
            period_counts: HashMap::new(),
            scores: HashMap::new(),
            hot_set: HashSet::new(),
        }
    }

    fn record_access(&mut self, fnid: FnId) {
        self.period_counts
            .entry(fnid)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.op_in_period += 1;
        if self.op_in_period >= self.period_ops {
            self.refresh();
        }
    }

    fn refresh(&mut self) {
        let mut all_fn_ids: HashSet<FnId> = HashSet::new();
        all_fn_ids.extend(self.scores.keys().copied());
        all_fn_ids.extend(self.period_counts.keys().copied());

        for fnid in all_fn_ids {
            let c_t = self.period_counts.remove(&fnid).unwrap_or(0) as f32;
            let prev = self.scores.get(&fnid).copied().unwrap_or(0.0);
            let s_t = self.alpha * c_t + (1.0 - self.alpha) * prev;
            self.scores.insert(fnid, s_t);
        }

        self.op_in_period = 0;
        self.rebuild_hot_set();
    }

    fn rebuild_hot_set(&mut self) {
        let mut sorted: Vec<(FnId, f32)> = self
            .scores
            .iter()
            .filter_map(|(fnid, score)| {
                if *score > 0.0 {
                    Some((*fnid, *score))
                } else {
                    None
                }
            })
            .collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        self.hot_set.clear();
        if sorted.is_empty() {
            return;
        }

        let total: f32 = sorted.iter().map(|(_, s)| *s).sum();
        if total <= 0.00001 {
            return;
        }

        let mut accum = 0.0;
        for (fnid, score) in sorted {
            self.hot_set.insert(fnid);
            accum += score;
            if accum / total >= self.cover_threshold {
                break;
            }
        }
    }

    fn is_hot(&self, fnid: FnId) -> bool {
        self.hot_set.contains(&fnid)
    }

    fn score(&self, fnid: FnId) -> f32 {
        self.scores.get(&fnid).copied().unwrap_or(0.0)
    }
}

fn global_controller() -> &'static Mutex<GlobalHotController> {
    static CTRL: OnceLock<Mutex<GlobalHotController>> = OnceLock::new();
    CTRL.get_or_init(|| Mutex::new(GlobalHotController::new()))
}

pub fn global_record_access(fnid: FnId) {
    let mut ctrl = global_controller().lock().unwrap();
    ctrl.record_access(fnid);
}

pub fn global_is_hot(fnid: FnId) -> bool {
    let ctrl = global_controller().lock().unwrap();
    ctrl.is_hot(fnid)
}

pub fn global_fn_score(fnid: FnId) -> f32 {
    let ctrl = global_controller().lock().unwrap();
    ctrl.score(fnid)
}

pub struct GVGCCache {
    capacity: usize,
    entries: HashMap<FnId, CacheEntry>,
    tick: u64,
    ttl_ticks: u64,
}

impl GVGCCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            tick: 0,
            // 用操作步数近似时间窗口，避免改动全局帧时钟。
            ttl_ticks: 120,
        }
    }

    fn next_tick(&mut self) {
        self.tick += 1;
    }

    fn migrate_generations(&mut self) {
        for (fnid, entry) in self.entries.iter_mut() {
            let hot = global_is_hot(*fnid);
            match (entry.gen, hot) {
                (Generation::Young, true) => {
                    entry.gen = Generation::Old;
                }
                (Generation::Old, false) => {
                    entry.gen = Generation::Young;
                    entry.last_touch_tick = self.tick;
                }
                _ => {}
            }
        }
    }

    fn evict_expired_young(
        &mut self,
        mut can_be_evict: impl FnMut(&FnId) -> bool,
    ) -> Vec<FnId> {
        let expired: Vec<FnId> = self
            .entries
            .iter()
            .filter_map(|(fnid, entry)| {
                if entry.gen != Generation::Young {
                    return None;
                }
                if self.tick.saturating_sub(entry.last_touch_tick) < self.ttl_ticks {
                    return None;
                }
                if !can_be_evict(fnid) {
                    return None;
                }
                Some(*fnid)
            })
            .collect();

        for fnid in expired.iter() {
            self.entries.remove(fnid);
        }
        expired
    }

    fn evict_one_young_by_utility(
        &mut self,
        mut can_be_evict: impl FnMut(&FnId) -> bool,
    ) -> Option<FnId> {
        let victim = self
            .entries
            .iter()
            .filter(|(fnid, e)| e.gen == Generation::Young && can_be_evict(fnid))
            .min_by(|(_, a), (_, b)| {
                let ua = a.hit_count as f32;
                let ub = b.hit_count as f32;
                ua.partial_cmp(&ub)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.last_touch_tick.cmp(&b.last_touch_tick))
            })
            .map(|(fnid, _)| *fnid);

        if let Some(v) = victim {
            self.entries.remove(&v);
            return Some(v);
        }
        None
    }
}

impl InstanceCachePolicy<FnId> for GVGCCache {
    fn get(&mut self, key: FnId) -> Option<FnId> {
        self.next_tick();
        global_record_access(key);
        self.migrate_generations();

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.hit_count += 1;
            entry.last_touch_tick = self.tick;
            return Some(key);
        }
        None
    }

    fn put(
        &mut self,
        key: FnId,
        mut can_be_evict: Box<dyn FnMut(&FnId) -> bool>,
    ) -> (Option<FnId>, bool) {
        self.next_tick();
        global_record_access(key);
        self.migrate_generations();

        if let Some(entry) = self.entries.get_mut(&key) {
            entry.hit_count += 1;
            entry.last_touch_tick = self.tick;
            return (None, true);
        }

        let mut evicted: Option<FnId> = None;
        if self.entries.len() >= self.capacity {
            let _ = self.evict_expired_young(|fnid| can_be_evict(fnid));
        }
        while self.entries.len() >= self.capacity {
            let one = self.evict_one_young_by_utility(|fnid| can_be_evict(fnid));
            if one.is_none() {
                return (None, false);
            }
            evicted = one;
        }

        let gen = if global_is_hot(key) {
            Generation::Old
        } else {
            Generation::Young
        };
        self.entries.insert(
            key,
            CacheEntry {
                gen,
                hit_count: 1,
                last_touch_tick: self.tick,
            },
        );

        (evicted, true)
    }

    fn remove_all(&mut self, key: &FnId) -> bool {
        self.entries.remove(key).is_some()
    }
}

unsafe impl Send for GVGCCache {}

