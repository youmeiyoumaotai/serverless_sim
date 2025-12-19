use std::{
    collections::{HashMap, VecDeque},
    cmp::Ordering,
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

/// Jiagu调度器 - 实现预决策调度和双阶段扩缩容
/// 基于USENIX ATC '24论文"Harmonizing Efficiency and Practicability: Optimizing Resource Utilization in Serverless Computing with Jiagu"
pub struct JiaguScheduler {
    // 预决策调度相关状态
    predicted_requests: HashMap<FnId, f64>, // 函数的预测请求率
    pre_decisions: HashMap<FnId, Vec<NodeId>>, // 预决策的调度结果
    
    // 双阶段扩缩容相关状态
    saturated_instances: HashMap<NodeId, HashMap<FnId, usize>>, // 每个节点上每个函数的饱和实例数
    instance_adjustments: HashMap<NodeId, HashMap<FnId, i32>>, // 实例调整记录
    
    // 性能监控
    rps_monitor: HashMap<FnId, VecDeque<f64>>, // RPS监控窗口
    latency_monitor: HashMap<FnId, VecDeque<f64>>, // 延迟监控窗口
    
    // 调度决策缓存
    decision_cache: HashMap<FnId, (NodeId, u64)>, // 函数到节点的映射缓存(节点ID, 时间戳)
    
    // 配置参数
    prediction_window: usize, // 预测窗口大小
    adjustment_threshold: f64, // 调整阈值
    cache_ttl: u64, // 缓存TTL(毫秒)
}

impl JiaguScheduler {
    pub fn new() -> Self {
        Self {
            predicted_requests: HashMap::new(),
            pre_decisions: HashMap::new(),
            saturated_instances: HashMap::new(),
            instance_adjustments: HashMap::new(),
            rps_monitor: HashMap::new(),
            latency_monitor: HashMap::new(),
            decision_cache: HashMap::new(),
            prediction_window: 10,
            adjustment_threshold: 0.8,
            cache_ttl: 5000, // 5秒缓存
        }
    }
    
    /// 预决策调度 - 基于历史数据预测未来请求并预先做出调度决策
    fn pre_decision_scheduling(&mut self, env: &SimEnvObserve) {
        // 更新RPS监控数据
        self.update_rps_monitoring(env);
        
        // 预测未来请求率
        self.predict_future_requests();
        
        // 基于预测结果生成预决策
        self.generate_pre_decisions(env);
    }
    
    /// 更新RPS监控数据
    fn update_rps_monitoring(&mut self, env: &SimEnvObserve) {
        let current_time = env.core().current_frame() as f64;
        
        // 统计当前帧的请求数
        for (_req_id, req) in env.core().requests().iter() {
            let dag = env.dag(req.dag_i);
            for node_idx in dag.dag_inner.raw_nodes().iter().enumerate().map(|(i, _)| NodeIndex::new(i)) {
                if let Some(fnid) = dag.dag_inner.node_weight(node_idx) {
                    let fnid = *fnid;
                let rps_window = self.rps_monitor.entry(fnid).or_insert_with(VecDeque::new);
                
                // 添加当前RPS数据点
                rps_window.push_back(1.0); // 简化：每个请求贡献1个RPS
                
                // 保持窗口大小
                if rps_window.len() > self.prediction_window {
                    rps_window.pop_front();
                    }
            }
            }
        }
    }
    
    /// 预测未来请求率
    fn predict_future_requests(&mut self) {
        for (fnid, rps_window) in &self.rps_monitor {
            if rps_window.len() >= 3 {
                // 使用简单的移动平均预测
                let avg_rps: f64 = rps_window.iter().sum::<f64>() / rps_window.len() as f64;
                
                // 考虑趋势因子
                let trend_factor = if rps_window.len() >= 2 {
                    let recent = rps_window.iter().rev().take(3).sum::<f64>() / 3.0;
                    let older = rps_window.iter().take(3).sum::<f64>() / 3.0;
                    if older > 0.0 { recent / older } else { 1.0 }
                } else {
                    1.0
                };
                
                self.predicted_requests.insert(*fnid, avg_rps * trend_factor);
            }
        }
    }
    
    /// 生成预决策
    fn generate_pre_decisions(&mut self, env: &SimEnvObserve) {
        self.pre_decisions.clear();
        
        for (fnid, predicted_rps) in &self.predicted_requests {
            if *predicted_rps > 0.1 { // 只为有足够预测请求的函数生成预决策
                let suitable_nodes = self.find_suitable_nodes_for_function(*fnid, env);
                self.pre_decisions.insert(*fnid, suitable_nodes);
            }
        }
    }
    
    /// 查找适合函数的节点
    fn find_suitable_nodes_for_function(&self, fnid: FnId, env: &SimEnvObserve) -> Vec<NodeId> {
        let mut suitable_nodes = Vec::new();
        let func = env.func(fnid);
        
        for node in env.nodes().iter() {
            // 检查节点资源是否足够
            if self.can_node_handle_function(node, func.mem, func.cpu) {
                suitable_nodes.push(node.node_id());
            }
        }
        
        // 按资源利用率排序，优先选择资源利用率较低的节点
        suitable_nodes.sort_by(|a, b| {
            let node_a = env.node(*a);
            let node_b = env.node(*b);
            let util_a = self.calculate_node_utilization(&*node_a);
            let util_b = self.calculate_node_utilization(&*node_b);
            util_a.partial_cmp(&util_b).unwrap_or(Ordering::Equal)
        });
        
        suitable_nodes
    }
    
    /// 检查节点是否能处理函数
    fn can_node_handle_function(&self, node: &Node, mem_req: f32, cpu_req: f32) -> bool {
        let available_mem = node.rsc_limit.mem - *node.unready_mem_mut();
        let available_cpu = node.rsc_limit.cpu - node.rsc_limit.cpu * (node.all_task_cnt() as f32 / 100.0);
        
        available_mem >= mem_req && available_cpu >= cpu_req
    }
    
    /// 计算节点利用率
    fn calculate_node_utilization(&self, node: &Node) -> f32 {
        let mem_util = *node.unready_mem_mut() / node.rsc_limit.mem;
        let cpu_util = node.all_task_cnt() as f32 / 100.0; // 简化的CPU利用率计算
        (mem_util + cpu_util) / 2.0
    }
    
    /// 双阶段扩缩容 - 第一阶段：频繁调整实例
    fn dual_staged_scaling_phase1(&mut self, env: &SimEnvObserve) {
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            
            for (fnid, _) in node.fn_containers.borrow().iter() {
                // 计算当前函数的饱和实例数
                let current_instances = env.fn_container_cnt(*fnid);
                let predicted_rps = self.predicted_requests.get(fnid).unwrap_or(&0.0);
                
                // 基于预测RPS计算所需实例数
                let required_instances = self.calculate_required_instances(*predicted_rps, *fnid, env);
                
                // 记录饱和实例数
                self.saturated_instances
                    .entry(node_id)
                    .or_insert_with(HashMap::new)
                    .insert(*fnid, required_instances);
                
                // 计算调整量
                let adjustment = required_instances as i32 - current_instances as i32;
                if adjustment.abs() > 0 {
                    self.instance_adjustments
                        .entry(node_id)
                        .or_insert_with(HashMap::new)
                        .insert(*fnid, adjustment);
                }
            }
        }
    }
    
    /// 计算所需实例数
    fn calculate_required_instances(&self, predicted_rps: f64, fnid: FnId, env: &SimEnvObserve) -> usize {
        if predicted_rps <= 0.0 {
            return 1; // 至少保持一个实例
        }
        
        let func = env.func(fnid);
        // 假设每个实例每秒可以处理的请求数（基于函数的资源需求）
        let instance_capacity = 10.0 / (func.cpu as f64 + func.mem as f64 / 100.0); // 简化计算
        
        let required = (predicted_rps / instance_capacity).ceil() as usize;
        std::cmp::max(1, std::cmp::min(required, 10)) // 限制在1-10个实例之间
    }
    
    /// 双阶段扩缩容 - 第二阶段：最小化开销
    fn dual_staged_scaling_phase2(&mut self, _env: &SimEnvObserve) {
        // 合并相似的调整操作以减少开销
        let mut consolidated_adjustments: HashMap<NodeId, HashMap<FnId, i32>> = HashMap::new();
        
        for (node_id, fn_adjustments) in &self.instance_adjustments {
            for (fnid, adjustment) in fn_adjustments {
                if adjustment.abs() >= 2 { // 只处理较大的调整
                    consolidated_adjustments
                        .entry(*node_id)
                        .or_insert_with(HashMap::new)
                        .insert(*fnid, *adjustment);
                }
            }
        }
        
        // 更新调整记录
        self.instance_adjustments = consolidated_adjustments;
    }
    
    /// 执行调度决策
    fn execute_scheduling_decision(
        &mut self,
        fnid: FnId,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
        req_id: usize,
    ) -> bool {
        // 首先检查缓存的决策
        let current_time = env.core().current_frame() as u64;
        if let Some((cached_node, timestamp)) = self.decision_cache.get(&fnid) {
            if current_time - timestamp < self.cache_ttl {
                // 使用缓存的决策
                return self.send_schedule_command(*cached_node, req_id, fnid, cmd_distributor);
            }
        }
        
        // 使用预决策结果
        if let Some(pre_decided_nodes) = self.pre_decisions.get(&fnid) {
            if !pre_decided_nodes.is_empty() {
                let selected_node = pre_decided_nodes[0]; // 选择第一个预决策节点
                
                // 更新缓存
                self.decision_cache.insert(fnid, (selected_node, current_time));
                
                return self.send_schedule_command(selected_node, req_id, fnid, cmd_distributor);
            }
        }
        
        // 回退到传统调度
        self.fallback_scheduling(fnid, env, cmd_distributor, req_id)
    }
    
    /// 回退调度策略
    fn fallback_scheduling(
        &mut self,
        fnid: FnId,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
        req_id: usize,
    ) -> bool {
        // 简单的负载均衡策略
        let nodes_ref = env.nodes();
        let nodes: Vec<_> = nodes_ref.iter().collect();
        if nodes.is_empty() {
            return false;
        }
        
        // 选择任务数最少的节点
        let best_node = nodes.iter().min_by_key(|node| node.all_task_cnt());
        
        if let Some(node) = best_node {
            let current_time = env.core().current_frame() as u64;
            self.decision_cache.insert(fnid, (node.node_id(), current_time));
            return self.send_schedule_command(node.node_id(), req_id, fnid, cmd_distributor);
        }
        
        false
    }
    
    /// 发送调度命令
    fn send_schedule_command(
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
            Ok(_) => true,
            Err(e) => {
                log::warn!("Failed to send Jiagu schedule command for fn {} to node {}: {:?}", fnid, node_id, e);
                false
            }
        }
    }
}

impl Scheduler for JiaguScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // Jiagu调度算法的主要流程
        
        // 1. 预决策调度
        self.pre_decision_scheduling(env);
        
        // 2. 双阶段扩缩容
        self.dual_staged_scaling_phase1(env);
        self.dual_staged_scaling_phase2(env);
        
        // 3. 处理当前请求
        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            
            // 为每个函数执行调度决策
            for fnid in fns {
                self.execute_scheduling_decision(fnid, env, cmd_distributor, req.req_id);
            }
        }
    }
}