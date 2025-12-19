use std::{
    collections::{HashMap, VecDeque, BinaryHeap},
    cmp::{Ordering, Reverse},
};
use daggy::NodeIndex;

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechType, MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, Node, NodeId},
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

/// Worker状态信息
#[derive(Debug, Clone)]
struct WorkerState {
    node_id: NodeId,
    active_connections: usize,
    idle_instances: HashMap<FnId, usize>, // 每个函数类型的空闲实例数
    last_update_time: u64,
}

impl PartialEq for WorkerState {
    fn eq(&self, other: &Self) -> bool {
        self.active_connections == other.active_connections
    }
}

impl Eq for WorkerState {}

impl PartialOrd for WorkerState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorkerState {
    fn cmp(&self, other: &Self) -> Ordering {
        // 优先选择连接数较少的worker（使用Reverse实现最小堆）
        other.active_connections.cmp(&self.active_connections)
    }
}

/// Hiku调度器 - 实现基于拉取的调度算法
/// 基于HPDC '25论文"Hiku: Pull-Based Scheduling for Serverless Computing"
pub struct HikuScheduler {
    // 每个函数类型的优先队列，存储有空闲实例的worker
    function_idle_queues: HashMap<FnId, BinaryHeap<WorkerState>>,
    
    // Worker状态跟踪
    worker_states: HashMap<NodeId, WorkerState>,
    
    // 性能监控
    cold_start_count: HashMap<FnId, usize>,
    total_requests: HashMap<FnId, usize>,
    response_times: HashMap<FnId, VecDeque<f64>>,
    
    // 配置参数
    monitoring_window_size: usize,
    worker_pull_interval: u64, // worker拉取任务的间隔（毫秒）
    load_balance_threshold: f64, // 负载均衡阈值
}

impl HikuScheduler {
    pub fn new() -> Self {
        Self {
            function_idle_queues: HashMap::new(),
            worker_states: HashMap::new(),
            cold_start_count: HashMap::new(),
            total_requests: HashMap::new(),
            response_times: HashMap::new(),
            monitoring_window_size: 100,
            worker_pull_interval: 100, // 100ms
            load_balance_threshold: 0.8,
        }
    }
    
    /// 更新worker状态
    fn update_worker_states(&mut self, env: &SimEnvObserve) {
        let current_time = env.core().current_frame() as u64;
        
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            let active_connections = node.all_task_cnt();
            
            // 计算每个函数类型的空闲实例数
            let mut idle_instances = HashMap::new();
            for (fnid, container) in node.fn_containers.borrow().iter() {
                // 检查容器是否处于运行状态且空闲
                if container.is_running() && container.is_idle() {
                    idle_instances.insert(*fnid, 1); // 每个容器算作1个空闲实例
                }
            }
            
            let worker_state = WorkerState {
                node_id,
                active_connections,
                idle_instances,
                last_update_time: current_time,
            };
            
            self.worker_states.insert(node_id, worker_state);
        }
    }
    
    /// 计算节点上指定函数的忙碌实例数
    fn count_busy_instances(&self, node_id: NodeId, fnid: FnId, env: &SimEnvObserve) -> usize {
        // 简化实现：检查指定函数的容器是否忙碌
        let node = env.node(node_id);
        let fn_containers = node.fn_containers.borrow();
        
        if let Some(container) = fn_containers.get(&fnid) {
            // 如果容器正在运行且不空闲，则认为是忙碌的
            if container.is_running() && !container.is_idle() {
                return 1;
            }
        }
        0
    }
    
    /// 更新函数的空闲队列
    fn update_function_idle_queues(&mut self) {
        // 清空所有队列
        self.function_idle_queues.clear();
        
        // 重新构建队列
        for worker_state in self.worker_states.values() {
            for (fnid, idle_count) in &worker_state.idle_instances {
                if *idle_count > 0 {
                    let queue = self.function_idle_queues.entry(*fnid).or_insert_with(BinaryHeap::new);
                    queue.push(worker_state.clone());
                }
            }
        }
    }
    
    /// 基于拉取的调度决策
    fn pull_based_scheduling(
        &mut self,
        fnid: FnId,
        req_id: usize,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) -> bool {
        // 1. 首先检查是否有空闲的warm实例
        let mut selected_worker: Option<NodeId> = None;
        
        if let Some(queue) = self.function_idle_queues.get_mut(&fnid) {
            while let Some(worker_state) = queue.pop() {
                // 验证worker状态是否仍然有效（简化检查，避免借用冲突）
                if let Some(current_state) = self.worker_states.get(&worker_state.node_id) {
                    if let Some(idle_count) = current_state.idle_instances.get(&fnid) {
                        if *idle_count > 0 {
                            selected_worker = Some(worker_state.node_id);
                            break;
                        }
                    }
                }
            }
        }
        
        if let Some(node_id) = selected_worker {
            // 找到合适的worker，分配任务
            let success = self.assign_task_to_worker(node_id, req_id, fnid, cmd_distributor);
            if success {
                // 更新统计信息
                self.update_request_stats(fnid, false); // 不是冷启动
            }
            return success;
        }
        
        // 2. 没有warm实例，使用回退机制（最少连接调度）
        self.fallback_least_connections_scheduling(fnid, req_id, env, cmd_distributor)
    }
    
    /// 分配任务到worker
    fn assign_task_to_worker(
        &self,
        node_id: NodeId,
        req_id: usize,
        fnid: FnId,
        cmd_distributor: &MechCmdDistributor,
    ) -> bool {
        match cmd_distributor.send(MechScheduleOnceRes::ScheCmd(ScheCmd {
            nid: node_id,
            reqid: req_id,
            fnid,
            memlimit: None,
        })) {
            Ok(_) => {
                log::debug!("Hiku: Assigned task {} (fn {}) to worker {}", req_id, fnid, node_id);
                true
            }
            Err(e) => {
                log::warn!("Hiku: Failed to assign task {} (fn {}) to worker {}: {:?}", req_id, fnid, node_id, e);
                false
            }
        }
    }
    
    /// 回退机制：最少连接调度
    fn fallback_least_connections_scheduling(
        &mut self,
        fnid: FnId,
        req_id: usize,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) -> bool {
        let nodes_ref = env.nodes();
        let nodes: Vec<_> = nodes_ref.iter().collect();
        
        if nodes.is_empty() {
            return false;
        }
        
        // 选择连接数最少的节点
        let best_node = nodes.iter().min_by_key(|node| node.all_task_cnt());
        
        if let Some(node) = best_node {
            let success = self.assign_task_to_worker(node.node_id(), req_id, fnid, cmd_distributor);
            if success {
                // 这是一个冷启动
                self.update_request_stats(fnid, true);
                log::debug!("Hiku: Fallback scheduling - assigned task {} (fn {}) to node {} (cold start)", 
                           req_id, fnid, node.node_id());
            }
            return success;
        }
        
        false
    }
    
    /// 更新请求统计信息
    fn update_request_stats(&mut self, fnid: FnId, is_cold_start: bool) {
        *self.total_requests.entry(fnid).or_insert(0) += 1;
        
        if is_cold_start {
            *self.cold_start_count.entry(fnid).or_insert(0) += 1;
        }
    }
    
    /// 计算冷启动率
    fn calculate_cold_start_rate(&self, fnid: FnId) -> f64 {
        let total = self.total_requests.get(&fnid).unwrap_or(&0);
        let cold_starts = self.cold_start_count.get(&fnid).unwrap_or(&0);
        
        if *total > 0 {
            *cold_starts as f64 / *total as f64
        } else {
            0.0
        }
    }
    
    /// 计算负载均衡指标
    fn calculate_load_balance_metric(&self) -> f64 {
        if self.worker_states.is_empty() {
            return 1.0; // 完美均衡
        }
        
        let connections: Vec<usize> = self.worker_states.values()
            .map(|state| state.active_connections)
            .collect();
        
        if connections.is_empty() {
            return 1.0;
        }
        
        let mean = connections.iter().sum::<usize>() as f64 / connections.len() as f64;
        let variance = connections.iter()
            .map(|&x| {
                let diff = x as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / connections.len() as f64;
        
        let std_dev = variance.sqrt();
        
        // 变异系数的倒数作为负载均衡指标（越接近1越均衡）
        if mean > 0.0 {
            1.0 / (1.0 + std_dev / mean)
        } else {
            1.0
        }
    }
    
    /// 打印性能统计信息
    fn print_performance_stats(&self) {
        log::info!("=== Hiku Scheduler Performance Stats ===");
        
        for (fnid, total) in &self.total_requests {
            let cold_start_rate = self.calculate_cold_start_rate(*fnid);
            log::info!("Function {}: {} requests, {:.2}% cold start rate", 
                      fnid, total, cold_start_rate * 100.0);
        }
        
        let load_balance_metric = self.calculate_load_balance_metric();
        log::info!("Load balance metric: {:.3} (1.0 = perfect balance)", load_balance_metric);
        
        log::info!("Active workers: {}", self.worker_states.len());
    }
}

impl Scheduler for HikuScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // Hiku调度算法的主要流程
        
        // 1. 更新worker状态
        self.update_worker_states(env);
        
        // 2. 更新函数的空闲队列
        self.update_function_idle_queues();
        
        // 3. 处理当前请求
        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            
            // 为每个函数执行基于拉取的调度
            for fnid in fns {
                self.pull_based_scheduling(fnid, req.req_id, env, cmd_distributor);
            }
        }
        
        // 4. 定期打印性能统计（每1000帧）
        if env.core().current_frame() % 1000 == 0 {
            self.print_performance_stats();
        }
    }
}