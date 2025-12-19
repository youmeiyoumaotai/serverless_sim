use std::{
    collections::{HashMap, HashSet, VecDeque},
    cmp::Ordering,
};
use daggy::Walker;

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::ReqId,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

/// Orion DAG性能模型 - 基于关键路径分析的性能预测
#[derive(Clone, Debug)]
pub struct OrionDagPerformanceModel {
    /// 函数执行时间估计 (ms)
    pub execution_time: HashMap<FnId, f32>,
    /// 数据传输时间估计 (ms)
    pub transfer_time: HashMap<FnId, f32>,
    /// 关键路径长度 (ms)
    pub critical_path_length: f32,
    /// 函数优先级 (基于关键路径贡献)
    pub function_priority: HashMap<FnId, f32>,
    /// 预热收益估计 (ms)
    pub warmup_benefit: HashMap<FnId, f32>,
}

/// Orion预预热策略
#[derive(Clone, Debug)]
pub struct OrionPreWarmStrategy {
    /// 预热候选函数集合
    pub prewarm_candidates: HashSet<FnId>,
    /// 预热优先级 (基于DAG分析)
    pub prewarm_priority: HashMap<FnId, f32>,
    /// 预热时间窗口 (ms)
    pub prewarm_window: f32,
    /// 预热资源限制
    pub prewarm_resource_limit: f32,
}

/// Orion节点选择策略
#[derive(Clone, Debug)]
pub struct OrionNodeSelection {
    /// 节点性能评分
    pub node_performance_score: HashMap<NodeId, f32>,
    /// 节点预热状态
    pub node_warmup_state: HashMap<NodeId, bool>,
    /// 节点负载均衡权重
    pub node_load_balance_weight: HashMap<NodeId, f32>,
    /// 节点网络延迟
    pub node_network_latency: HashMap<NodeId, f32>,
}

/// Orion调度器配置
#[derive(Clone, Debug)]
pub struct OrionConfig {
    /// 关键路径权重
    pub critical_path_weight: f32,
    /// 预热收益权重
    pub prewarm_benefit_weight: f32,
    /// 负载均衡权重
    pub load_balance_weight: f32,
    /// 网络延迟权重
    pub network_latency_weight: f32,
    /// 预热时间窗口 (ms)
    pub prewarm_time_window: f32,
    /// 预热资源限制比例
    pub prewarm_resource_limit_ratio: f32,
    /// 性能预测置信度阈值
    pub performance_confidence_threshold: f32,
}

impl Default for OrionConfig {
    fn default() -> Self {
        Self {
            critical_path_weight: 0.4,
            prewarm_benefit_weight: 0.3,
            load_balance_weight: 0.2,
            network_latency_weight: 0.1,
            prewarm_time_window: 1000.0, // 1秒预热窗口
            prewarm_resource_limit_ratio: 0.2, // 20%资源用于预热
            performance_confidence_threshold: 0.8,
        }
    }
}

/// Orion调度器 - 基于DAG性能模型和预预热优化
pub struct OrionScheduler {
    /// 配置参数
    config: OrionConfig,
    /// DAG性能模型
    dag_performance_model: HashMap<ReqId, OrionDagPerformanceModel>,
    /// 预预热策略
    prewarm_strategy: OrionPreWarmStrategy,
    /// 节点选择策略
    node_selection: OrionNodeSelection,
    /// 已调度的函数-请求映射
    scheduled_fn_req_pairs: HashSet<(FnId, ReqId)>,
    /// 预热历史记录
    prewarm_history: VecDeque<(FnId, NodeId, f32)>,
    /// 性能预测缓存
    performance_cache: HashMap<(FnId, NodeId), f32>,
}

impl OrionScheduler {
    pub fn new() -> Self {
        Self {
            config: OrionConfig::default(),
            dag_performance_model: HashMap::new(),
            prewarm_strategy: OrionPreWarmStrategy {
                prewarm_candidates: HashSet::new(),
                prewarm_priority: HashMap::new(),
                prewarm_window: 1000.0,
                prewarm_resource_limit: 0.2,
            },
            node_selection: OrionNodeSelection {
                node_performance_score: HashMap::new(),
                node_warmup_state: HashMap::new(),
                node_load_balance_weight: HashMap::new(),
                node_network_latency: HashMap::new(),
            },
            scheduled_fn_req_pairs: HashSet::new(),
            prewarm_history: VecDeque::new(),
            performance_cache: HashMap::new(),
        }
    }

    /// 构建DAG性能模型
    fn build_dag_performance_model(&mut self, req_id: ReqId, env: &SimEnvObserve) -> OrionDagPerformanceModel {
        let requests = env.core().requests();
        let req = requests.get(&req_id).expect(&format!("Orion调度器：未找到请求ID {}", req_id));
        let dag = env.dag(req.dag_i);
        
        let mut execution_time = HashMap::new();
        let mut transfer_time = HashMap::new();
        let mut function_priority = HashMap::new();
        let mut warmup_benefit = HashMap::new();
        
        // 计算每个函数的执行时间和传输时间
        let mut walker = dag.new_dag_walker();
        while let Some(func_g_i) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[func_g_i];
            let func = env.func(fnid);
            
            // 执行时间估计 (基于CPU需求)
            let node_count = env.node_cnt() as f32;
            let avg_node_cpu = env.nodes().iter().map(|n| n.rsc_limit.cpu).sum::<f32>() / node_count;
            let exec_time = (func.cpu / avg_node_cpu) * 1000.0; // 转换为毫秒
            execution_time.insert(fnid, exec_time);
            
            // 传输时间估计 (基于输出大小和固定带宽)
            let avg_bandwidth = 1000.0; // 固定带宽值，因为Node结构中没有bandwidth字段
            let transfer_time_ms = (func.out_put_size / avg_bandwidth) * 1000.0;
            transfer_time.insert(fnid, transfer_time_ms);
            
            // 预热收益估计 (基于冷启动时间)
            let warmup_benefit_ms = func.cold_start_time as f32 * 0.8; // 假设预热可减少80%冷启动时间
            warmup_benefit.insert(fnid, warmup_benefit_ms);
        }
        
        // 计算关键路径和函数优先级
        let critical_path_length = self.calculate_critical_path_length(&dag, &execution_time, &transfer_time);
        self.calculate_function_priorities(&dag, &execution_time, &transfer_time, &mut function_priority);
        
        OrionDagPerformanceModel {
            execution_time,
            transfer_time,
            critical_path_length,
            function_priority,
            warmup_benefit,
        }
    }

    /// 计算关键路径长度
    fn calculate_critical_path_length(&self, dag: &crate::fn_dag::FnDAG, execution_time: &HashMap<FnId, f32>, transfer_time: &HashMap<FnId, f32>) -> f32 {
        let mut node_times = HashMap::new();
        let mut walker = dag.new_dag_walker();
        
        // 计算每个节点的最早完成时间
        while let Some(func_g_i) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[func_g_i];
            let parents = dag.dag_inner.parents(func_g_i);
            
            let mut max_parent_time: f32 = 0.0;
            for parent in parents.iter(&dag.dag_inner) {
                let parent_fnid = dag.dag_inner[parent.1];
                if let Some(parent_time) = node_times.get(&parent_fnid) {
                    max_parent_time = max_parent_time.max(*parent_time);
                }
            }
            
            let exec_time = execution_time.get(&fnid).expect(&format!("Orion调度器：未找到函数{}的执行时间", fnid));
            let transfer_time = transfer_time.get(&fnid).expect(&format!("Orion调度器：未找到函数{}的传输时间", fnid));
            let total_time = max_parent_time + exec_time + transfer_time;
            node_times.insert(fnid, total_time);
        }
        
        // 关键路径长度是最大完成时间
        node_times.values().fold(0.0, |max, &time| max.max(time))
    }

    /// 计算函数优先级 (基于对关键路径的贡献)
    fn calculate_function_priorities(&self, dag: &crate::fn_dag::FnDAG, execution_time: &HashMap<FnId, f32>, transfer_time: &HashMap<FnId, f32>, priorities: &mut HashMap<FnId, f32>) {
        let mut walker = dag.new_dag_walker();
        let mut stack = Vec::new();
        
        // 第一遍：计算每个函数的执行+传输时间
        while let Some(func_g_i) = walker.next(&dag.dag_inner) {
            let fnid = dag.dag_inner[func_g_i];
            let exec_time = execution_time.get(&fnid).expect(&format!("Orion调度器：未找到函数{}的执行时间", fnid));
            let transfer_time = transfer_time.get(&fnid).expect(&format!("Orion调度器：未找到函数{}的传输时间", fnid));
            let total_time = exec_time + transfer_time;
            priorities.insert(fnid, total_time);
            stack.push(func_g_i);
        }
        
        // 第二遍：计算优先级 (基于后继函数的最大时间)
        while let Some(func_g_i) = stack.pop() {
            let fnid = dag.dag_inner[func_g_i];
            let children = dag.dag_inner.children(func_g_i);
            
            if let Some(max_child) = children.iter(&dag.dag_inner).max_by(|a, b| {
                let fnid_a = dag.dag_inner[a.1];
                let fnid_b = dag.dag_inner[b.1];
                priorities.get(&fnid_a).unwrap_or(&0.0).partial_cmp(priorities.get(&fnid_b).unwrap_or(&0.0)).unwrap_or(Ordering::Equal)
            }) {
                let max_child_fnid = dag.dag_inner[max_child.1];
                let max_child_priority = priorities.get(&max_child_fnid).unwrap_or(&0.0);
                let current_priority = priorities.get(&fnid).unwrap_or(&0.0);
                priorities.insert(fnid, current_priority + max_child_priority);
            }
        }
    }

    /// 更新预预热策略
    fn update_prewarm_strategy(&mut self, env: &SimEnvObserve) {
        self.prewarm_strategy.prewarm_candidates.clear();
        self.prewarm_strategy.prewarm_priority.clear();
        
        // 分析所有活跃请求的DAG，识别预热候选
        for (_, req) in env.core().requests().iter() {
            if let Some(performance_model) = self.dag_performance_model.get(&req.req_id) {
                for (fnid, priority) in &performance_model.function_priority {
                    // 高优先级函数作为预热候选
                    if *priority > self.config.performance_confidence_threshold * performance_model.critical_path_length {
                        self.prewarm_strategy.prewarm_candidates.insert(*fnid);
                        self.prewarm_strategy.prewarm_priority.insert(*fnid, *priority);
                    }
                }
            }
        }
    }

    /// 更新节点选择策略
    fn update_node_selection(&mut self, env: &SimEnvObserve) {
        self.node_selection.node_performance_score.clear();
        self.node_selection.node_warmup_state.clear();
        self.node_selection.node_load_balance_weight.clear();
        self.node_selection.node_network_latency.clear();
        
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            
            // 计算节点性能评分
            let cpu_util = node.cpu / node.rsc_limit.cpu;
            let mem_util = node.unready_mem() / node.rsc_limit.mem;
            let load_score = 1.0 - (cpu_util + mem_util) / 2.0;
            
            // 检查预热状态
            let has_warm_containers = !node.fn_containers.borrow().is_empty();
            
            // 网络延迟估计 (基于节点位置)
            let network_latency = 10.0 + (node_id as f32 * 2.0); // 简化的网络延迟模型
            
            self.node_selection.node_performance_score.insert(node_id, load_score);
            self.node_selection.node_warmup_state.insert(node_id, has_warm_containers);
            self.node_selection.node_load_balance_weight.insert(node_id, 1.0 - cpu_util);
            self.node_selection.node_network_latency.insert(node_id, network_latency);
        }
    }

    /// 选择最佳节点进行调度
    fn select_best_node(&self, fnid: FnId, env: &SimEnvObserve) -> NodeId {
        let func = env.func(fnid);
        let mut best_node = 0;
        let mut best_score = f32::NEG_INFINITY;
        
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            
            // 检查资源约束
            if node.cpu + func.cpu > node.rsc_limit.cpu || 
               node.unready_mem() + func.mem > node.rsc_limit.mem {
                continue;
            }
            
            // 计算综合评分
            let performance_score = self.node_selection.node_performance_score.get(&node_id).unwrap_or(&0.0);
            let load_balance_score = self.node_selection.node_load_balance_weight.get(&node_id).unwrap_or(&0.0);
            let network_score = 1.0 / (1.0 + self.node_selection.node_network_latency.get(&node_id).unwrap_or(&100.0));
            
            // 预热收益
            let warmup_benefit = if *self.node_selection.node_warmup_state.get(&node_id).unwrap_or(&false) {
                0.8 // 有预热容器时的收益
            } else {
                0.0
            };
            
            // 综合评分计算
            let total_score = self.config.critical_path_weight * performance_score +
                            self.config.load_balance_weight * load_balance_score +
                            self.config.network_latency_weight * network_score +
                            self.config.prewarm_benefit_weight * warmup_benefit;
            
            if total_score > best_score {
                best_score = total_score;
                best_node = node_id;
            }
        }
        
        best_node
    }

    /// 执行预热策略
    fn execute_prewarm_strategy(&mut self, env: &SimEnvObserve, cmd_distributor: &MechCmdDistributor) {
        // 按优先级排序预热候选
        let mut sorted_candidates: Vec<_> = self.prewarm_strategy.prewarm_candidates.iter()
            .map(|&fnid| (fnid, self.prewarm_strategy.prewarm_priority.get(&fnid).unwrap_or(&0.0)))
            .collect();
        sorted_candidates.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(Ordering::Equal));
        
        // 限制预热资源使用
        let mut prewarm_resource_used = 0.0;
        let total_resource = env.nodes().iter().map(|n| n.rsc_limit.cpu).sum::<f32>();
        let prewarm_limit = total_resource * self.config.prewarm_resource_limit_ratio;
        
        for (fnid, _priority) in sorted_candidates {
            if prewarm_resource_used >= prewarm_limit {
                break;
            }
            
            let func = env.func(fnid);
            let best_node = self.select_best_node(fnid, env);
            
            // 检查预热资源限制
            let node = env.node(best_node);
            if node.cpu + func.cpu <= node.rsc_limit.cpu && 
               node.unready_mem() + func.mem <= node.rsc_limit.mem {
                
                // 发送预热命令 - 注意：预热不应该触发on_task_scheduled事件
                // 这里我们暂时跳过预热，因为它可能导致fn_metric问题
                // cmd_distributor
                //     .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                //         nid: best_node,
                //         reqid: 0, // 预热请求ID为0
                //         fnid,
                //         memlimit: None,
                //     }))
                //     .unwrap_or_else(|e| log::warn!("Failed to send prewarm command: {:?}", e));
                
                prewarm_resource_used += func.cpu;
                
                // 记录预热历史
                self.prewarm_history.push_back((fnid, best_node, 0.0));
                if self.prewarm_history.len() > 100 {
                    self.prewarm_history.pop_front();
                }
            }
        }
    }
}

impl Scheduler for OrionScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 安全检查
        if env.core().dags().is_empty() || env.core().fns().is_empty() || env.core().requests().is_empty() {
            return;
        }

        // 更新节点选择策略
        self.update_node_selection(env);
        
        // 为每个请求构建DAG性能模型
        for (_, req) in env.core().requests().iter() {
            if !self.dag_performance_model.contains_key(&req.req_id) {
                let performance_model = self.build_dag_performance_model(req.req_id, env);
                self.dag_performance_model.insert(req.req_id, performance_model);
            }
        }
        
        // 更新预预热策略
        self.update_prewarm_strategy(env);
        
        // 执行预热策略
        self.execute_prewarm_strategy(env, cmd_distributor);
        
        // 调度待执行的函数
        for (req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            
            if let Some(performance_model) = self.dag_performance_model.get(&req.req_id) {
                // 按优先级排序函数
                let mut sorted_fns: Vec<_> = fns.iter()
                    .map(|&fnid| (fnid, performance_model.function_priority.get(&fnid).unwrap_or(&0.0)))
                    .collect();
                sorted_fns.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(Ordering::Equal));
                
                // 调度每个函数
                for (fnid, _priority) in sorted_fns {
                    let key = (fnid, req.req_id);
                    if self.scheduled_fn_req_pairs.contains(&key) {
                        continue;
                    }
                    
                    // 安全检查：确保函数存在于请求的fn_metric中
                    if !req.fn_metric.contains_key(&fnid) {
                        log::warn!("Orion调度器：函数{}不在请求{}的fn_metric中，跳过调度", fnid, req.req_id);
                        continue;
                    }
                    
                    let best_node = self.select_best_node(fnid, env);
                    
                    // 发送调度命令
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                            nid: best_node,
                            reqid: req.req_id,
                            fnid,
                            memlimit: None,
                        }))
                        .unwrap_or_else(|e| log::warn!("Failed to send schedule command: {:?}", e));
                    
                    self.scheduled_fn_req_pairs.insert(key);
                }
            }
        }
        
        // 清理过期的性能模型
        let active_req_ids: HashSet<ReqId> = env.core().requests().keys().cloned().collect();
        self.dag_performance_model.retain(|req_id, _| active_req_ids.contains(req_id));
        
        // 清理过期的调度记录
        let active_req_ids_set: HashSet<ReqId> = active_req_ids;
        self.scheduled_fn_req_pairs.retain(|(_, req_id)| active_req_ids_set.contains(req_id));
    }
} 