use std::{
    collections::{HashMap, VecDeque, BinaryHeap},
    cmp::{Ordering, Reverse},
};
use daggy::NodeIndex;
use rand::Rng;

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechType, MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, Node, NodeId},
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

/// 容器状态信息
#[derive(Debug, Clone)]
struct ContainerState {
    node_id: NodeId,
    fn_id: FnId,
    is_cached: bool,
    memory_cost: f64,
    startup_delay: f64,
    last_access_time: f64,
}

impl PartialEq for ContainerState {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.fn_id == other.fn_id
    }
}

impl Eq for ContainerState {}

impl PartialOrd for ContainerState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ContainerState {
    fn cmp(&self, other: &Self) -> Ordering {
        // 优先级：内存成本低 > 启动延迟低 > 最近访问时间
        self.memory_cost.partial_cmp(&other.memory_cost)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.startup_delay.partial_cmp(&other.startup_delay).unwrap_or(Ordering::Equal))
            .then_with(|| other.last_access_time.partial_cmp(&self.last_access_time).unwrap_or(Ordering::Equal))
    }
}

/// 随机化策略参数
#[derive(Debug, Clone)]
struct RandomizedStrategy {
    cache_probability: f64,
    startup_probability: f64,
    distribution_weight: f64,
}

/// OCS调度器配置
#[derive(Debug, Clone)]
struct OCSConfig {
    memory_threshold: f64,
    startup_delay_threshold: f64,
    competitive_ratio: f64,
    cache_decay_factor: f64,
}

impl Default for OCSConfig {
    fn default() -> Self {
        Self {
            memory_threshold: 0.8,  // 内存使用阈值
            startup_delay_threshold: 100.0,  // 启动延迟阈值(ms)
            competitive_ratio: 2.0,  // 竞争比
            cache_decay_factor: 0.9,  // 缓存衰减因子
        }
    }
}

/// OCS调度器 - 基于IEEE TC '24论文的在线容器调度算法
/// "Online Container Scheduling With Fast Function Startup and Low Memory Cost in Edge Computing"
pub struct OCSScheduler {
    /// 配置参数
    config: OCSConfig,
    
    /// 容器状态跟踪
    container_states: HashMap<(NodeId, FnId), ContainerState>,
    
    /// 每个函数的随机化策略
    randomized_strategies: HashMap<FnId, RandomizedStrategy>,
    
    /// 节点内存使用情况
    node_memory_usage: HashMap<NodeId, f64>,
    
    /// 函数调用历史
    invocation_history: HashMap<FnId, VecDeque<f64>>,
    
    /// 性能统计
    total_startup_delay: f64,
    total_memory_cost: f64,
    total_requests: usize,
    
    /// 缓存决策历史
    cache_decisions: HashMap<FnId, VecDeque<bool>>,
}

impl Default for OCSScheduler {
    fn default() -> Self {
        Self {
            config: OCSConfig::default(),
            container_states: HashMap::new(),
            randomized_strategies: HashMap::new(),
            node_memory_usage: HashMap::new(),
            invocation_history: HashMap::new(),
            total_startup_delay: 0.0,
            total_memory_cost: 0.0,
            total_requests: 0,
            cache_decisions: HashMap::new(),
        }
    }
}

impl OCSScheduler {
    /// 创建新的OCS调度器
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 更新随机化策略
    fn update_randomized_strategies(&mut self, env: &SimEnvObserve) {
        for func in env.core().fns().iter() {
            let fn_id = func.fn_id;
            let strategy = self.randomized_strategies.entry(fn_id).or_insert_with(|| {
                RandomizedStrategy {
                    cache_probability: 0.5,
                    startup_probability: 0.7,
                    distribution_weight: 1.0,
                }
            });
            
            // 基于历史性能动态调整策略
            if let Some(history) = self.invocation_history.get(&fn_id) {
                if history.len() > 10 {
                    let avg_delay = history.iter().sum::<f64>() / history.len() as f64;
                    
                    // 如果平均延迟高，增加缓存概率
                    if avg_delay > self.config.startup_delay_threshold {
                        strategy.cache_probability = (strategy.cache_probability * 1.1).min(0.9);
                    } else {
                        strategy.cache_probability = (strategy.cache_probability * 0.95).max(0.1);
                    }
                }
            }
        }
    }
    
    /// 贪心子程序：调用分发和容器启动决策
    fn greedy_invocation_distribution(
        &mut self,
        fn_id: FnId,
        env: &SimEnvObserve,
    ) -> Option<NodeId> {
        let mut best_node = None;
        let mut best_score = f64::INFINITY;
        
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            
            // 检查节点是否有该函数的容器
            let has_container = node.container(fn_id).is_some();
            let memory_usage = self.node_memory_usage.get(&node_id).unwrap_or(&0.0);
            
            // 计算调度得分
            let mut score = 0.0;
            
            if has_container {
                // 如果有容器，优先考虑
                score += 10.0;
                
                // 检查容器是否空闲
                if let Some(container) = node.container(fn_id) {
                    if container.is_idle() {
                        score += 50.0; // 有空闲容器，大幅加分
                    }
                }
            } else {
                // 没有容器，考虑启动成本
                score += 1.0;
            }
            
            // 内存使用惩罚
            score += memory_usage * 10.0;
            
            // 节点负载惩罚
            let node_load = node.running_task_cnt() as f64;
            score += node_load * 2.0;
            
            if score < best_score {
                best_score = score;
                best_node = Some(node_id);
            }
        }
        
        best_node
    }
    
    /// 容器缓存决策
    fn make_caching_decision(
        &mut self,
        fn_id: FnId,
        node_id: NodeId,
        env: &SimEnvObserve,
    ) {
        let strategy = self.randomized_strategies.get(&fn_id)
            .cloned()
            .unwrap_or_else(|| RandomizedStrategy {
                cache_probability: 0.5,
                startup_probability: 0.7,
                distribution_weight: 1.0,
            });
        
        let memory_usage = self.node_memory_usage.get(&node_id).unwrap_or(&0.0);
        
        // 如果内存使用超过阈值，不缓存
        if *memory_usage > self.config.memory_threshold {
            return;
        }
        
        // 基于随机化策略和当前状态决定是否缓存
        let cache_decision = rand::random::<f64>() < strategy.cache_probability;
        
        // 记录缓存决策
        self.cache_decisions.entry(fn_id)
            .or_insert_with(VecDeque::new)
            .push_back(cache_decision);
        
        // 保持历史记录在合理范围内
        if let Some(decisions) = self.cache_decisions.get_mut(&fn_id) {
            if decisions.len() > 100 {
                decisions.pop_front();
            }
        }
    }
    
    /// 更新性能统计
    fn update_performance_stats(&mut self, fn_id: FnId, node_id: NodeId) {
        let startup_delay = 100.0; // 简化估算
        let memory_cost = 50.0; // 简化估算
        
        self.total_startup_delay += startup_delay;
        self.total_memory_cost += memory_cost;
        self.total_requests += 1;
        
        // 更新调用历史
        self.invocation_history.entry(fn_id)
            .or_insert_with(VecDeque::new)
            .push_back(startup_delay);
        
        // 保持历史记录在合理范围内
        if let Some(history) = self.invocation_history.get_mut(&fn_id) {
            if history.len() > 50 {
                history.pop_front();
            }
        }
        
        // 更新节点内存使用
        *self.node_memory_usage.entry(node_id).or_insert(0.0) += memory_cost;
    }
    
    /// 执行OCS调度算法
    fn execute_ocs_scheduling(
        &mut self,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 1. 更新随机化策略
        self.update_randomized_strategies(env);
        
        // 2. 遍历所有请求，进行调度决策
        for (_, req) in env.core().requests().iter() {
            let schedulable_fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::PreAllSched,
            );
            
            for fnid in schedulable_fns {
                // 3. 贪婪调用分发
                if let Some(target_node) = self.greedy_invocation_distribution(fnid, env) {
                    // 4. 容器缓存决策
                    self.make_caching_decision(fnid, target_node, env);
                    
                    // 5. 执行调度
                    cmd_distributor.send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                        nid: target_node,
                        reqid: req.req_id,
                        fnid,
                        memlimit: None,
                    })).unwrap();
                    
                    // 6. 更新性能统计
                    self.update_performance_stats(fnid, target_node);
                }
            }
        }
    }
    
    /// 打印性能统计信息
    fn print_performance_stats(&self) {
        if self.total_requests > 0 {
            let avg_startup_delay = self.total_startup_delay / self.total_requests as f64;
            let avg_memory_cost = self.total_memory_cost / self.total_requests as f64;
            
            println!("[OCS] Performance Stats:");
            println!("  Total Requests: {}", self.total_requests);
            println!("  Avg Startup Delay: {:.2}ms", avg_startup_delay);
            println!("  Avg Memory Cost: {:.2}MB", avg_memory_cost);
            println!("  Total Startup Delay: {:.2}ms", self.total_startup_delay);
            println!("  Total Memory Cost: {:.2}MB", self.total_memory_cost);
        }
    }
}

impl Scheduler for OCSScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // OCS调度算法的主要流程
        self.execute_ocs_scheduling(env, cmd_distributor);
        
        // 定期打印性能统计（每1000个请求）
        if self.total_requests % 1000 == 0 && self.total_requests > 0 {
            self.print_performance_stats();
        }
    }
}