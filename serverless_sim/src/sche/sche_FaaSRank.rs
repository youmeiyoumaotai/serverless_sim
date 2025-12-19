use std::{
    collections::{HashMap, VecDeque},
    cmp::Ordering,
};

use rand::Rng;

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

/// FaaSRank调度器 - 基于强化学习和神经网络的函数调度器
/// 实现Score-Rank-Select架构，用于优化serverless平台上的函数调度和放置
/// 基于ACSOS 2021论文"FaaSRank: Learning to Schedule Functions in Serverless Platforms"
pub struct FaaSRankScheduler {
    /// 神经网络状态特征
    network_state: NetworkState,
    /// 强化学习智能体
    rl_agent: RLAgent,
    /// Score-Rank-Select架构组件
    score_rank_select: ScoreRankSelect,
    /// 历史调度决策记录
    decision_history: VecDeque<SchedulingDecision>,
    /// 节点性能监控
    node_metrics: HashMap<NodeId, NodeMetrics>,
    /// 函数性能监控
    function_metrics: HashMap<FnId, FunctionMetrics>,
    /// 学习参数
    learning_config: LearningConfig,
}

/// 神经网络状态表示
#[derive(Clone, Debug)]
struct NetworkState {
    /// 节点状态向量
    node_states: HashMap<NodeId, Vec<f32>>,
    /// 函数状态向量
    function_states: HashMap<FnId, Vec<f32>>,
    /// 全局系统状态
    global_state: Vec<f32>,
}

/// 强化学习智能体
#[derive(Clone, Debug)]
struct RLAgent {
    /// 策略网络权重
    policy_weights: Vec<Vec<f32>>,
    /// 价值网络权重
    value_weights: Vec<Vec<f32>>,
    /// 学习率
    learning_rate: f32,
    /// 折扣因子
    discount_factor: f32,
    /// 探索率
    epsilon: f32,
}

/// Score-Rank-Select架构
#[derive(Clone, Debug)]
struct ScoreRankSelect {
    /// 评分函数参数
    scoring_params: ScoringParams,
    /// 排名权重
    ranking_weights: Vec<f32>,
    /// 选择策略参数
    selection_params: SelectionParams,
}

/// 评分参数
#[derive(Clone, Debug)]
struct ScoringParams {
    /// CPU利用率权重
    cpu_weight: f32,
    /// 内存利用率权重
    memory_weight: f32,
    /// 网络延迟权重
    network_weight: f32,
    /// 函数冷启动权重
    cold_start_weight: f32,
    /// 负载均衡权重
    load_balance_weight: f32,
}

/// 选择策略参数
#[derive(Clone, Debug)]
struct SelectionParams {
    /// 温度参数（用于softmax选择）
    temperature: f32,
    /// 多样性因子
    diversity_factor: f32,
    /// 贪婪选择概率
    greedy_probability: f32,
}

/// 调度决策记录
#[derive(Clone, Debug)]
struct SchedulingDecision {
    /// 函数ID
    function_id: FnId,
    /// 选择的节点ID
    selected_node: NodeId,
    /// 决策时的状态
    state: Vec<f32>,
    /// 决策得分
    score: f32,
    /// 实际奖励
    reward: Option<f32>,
    /// 时间戳
    timestamp: u64,
}

/// 节点性能指标
#[derive(Clone, Debug)]
struct NodeMetrics {
    /// CPU利用率历史
    cpu_utilization: VecDeque<f32>,
    /// 内存利用率历史
    memory_utilization: VecDeque<f32>,
    /// 网络延迟历史
    network_latency: VecDeque<f32>,
    /// 任务完成时间历史
    task_completion_times: VecDeque<f32>,
    /// 当前运行任务数
    current_tasks: usize,
    /// 平均响应时间
    avg_response_time: f32,
}

/// 函数性能指标
#[derive(Clone, Debug)]
struct FunctionMetrics {
    /// 执行时间历史
    execution_times: VecDeque<f32>,
    /// 冷启动次数
    cold_starts: usize,
    /// 热启动次数
    warm_starts: usize,
    /// 内存使用量
    memory_usage: f32,
    /// CPU使用量
    cpu_usage: f32,
    /// 调度频率
    scheduling_frequency: f32,
}

/// 学习配置参数
#[derive(Clone, Debug)]
struct LearningConfig {
    /// 批量大小
    batch_size: usize,
    /// 经验回放缓冲区大小
    replay_buffer_size: usize,
    /// 目标网络更新频率
    target_update_frequency: usize,
    /// 最小探索率
    min_epsilon: f32,
    /// 探索率衰减
    epsilon_decay: f32,
    /// 奖励缩放因子
    reward_scale: f32,
}

impl FaaSRankScheduler {
    pub fn new() -> Self {
        Self {
            network_state: NetworkState::new(),
            rl_agent: RLAgent::new(),
            score_rank_select: ScoreRankSelect::new(),
            decision_history: VecDeque::with_capacity(1000),
            node_metrics: HashMap::new(),
            function_metrics: HashMap::new(),
            learning_config: LearningConfig::default(),
        }
    }

    /// 更新系统状态
    fn update_state(&mut self, env: &SimEnvObserve) {
        // 更新节点状态
        for node in env.core().nodes().iter() {
            let node_id = node.node_id();
            let state_vector = self.extract_node_features(node, env);
            self.network_state.node_states.insert(node_id, state_vector);
            
            // 更新节点性能指标
            self.update_node_metrics(node_id, node, env);
        }

        // 更新函数状态
        for func in env.core().fns().iter() {
            let fn_id = func.fn_id;
            let state_vector = self.extract_function_features(func, env);
            self.network_state.function_states.insert(fn_id, state_vector);
            
            // 更新函数性能指标
            self.update_function_metrics(fn_id, func, env);
        }

        // 更新全局状态
        self.network_state.global_state = self.extract_global_features(env);
    }

    /// 提取节点特征
    fn extract_node_features(&self, node: &crate::node::Node, env: &SimEnvObserve) -> Vec<f32> {
        let mut features = Vec::new();
        
        // CPU利用率
        let cpu_utilization = node.all_task_cnt() as f32 / node.cpu as f32;
        features.push(cpu_utilization.min(1.0));
        
        // 内存利用率
        let memory_utilization = node.unready_mem() / node.rsc_limit.mem;
        features.push(memory_utilization);
        
        // 当前任务数
        features.push(node.all_task_cnt() as f32);
        
        // 网络连接数（简化为与其他节点的连接数）
        let network_connections = env.core().nodes().len() as f32 - 1.0;
        features.push(network_connections);
        
        // 历史平均响应时间
        let avg_response_time = self.node_metrics
            .get(&node.node_id())
            .map(|m| m.avg_response_time)
            .unwrap_or(0.0);
        features.push(avg_response_time);
        
        features
    }

    /// 提取函数特征
    fn extract_function_features(&self, func: &crate::fn_dag::Func, env: &SimEnvObserve) -> Vec<f32> {
        let mut features = Vec::new();
        
        // 函数内存需求
        features.push(func.mem as f32);
        
        // 函数CPU需求（简化为固定值）
        features.push(1.0);
        
        // 函数执行时间估计
        let exec_time = self.function_metrics
            .get(&func.fn_id)
            .and_then(|m| m.execution_times.back())
            .copied()
            .unwrap_or(100.0);
        features.push(exec_time);
        
        // 冷启动概率
        let cold_start_prob = self.function_metrics
            .get(&func.fn_id)
            .map(|m| {
                let total_starts = m.cold_starts + m.warm_starts;
                if total_starts > 0 {
                    m.cold_starts as f32 / total_starts as f32
                } else {
                    1.0
                }
            })
            .unwrap_or(1.0);
        features.push(cold_start_prob);
        
        // 调度频率
        let scheduling_freq = self.function_metrics
            .get(&func.fn_id)
            .map(|m| m.scheduling_frequency)
            .unwrap_or(0.0);
        features.push(scheduling_freq);
        
        features
    }

    /// 提取全局特征
    fn extract_global_features(&self, env: &SimEnvObserve) -> Vec<f32> {
        let mut features = Vec::new();
        
        // 系统总负载
        let total_tasks: usize = env.core().nodes().iter()
            .map(|n| n.all_task_cnt())
            .sum();
        features.push(total_tasks as f32);
        
        // 活跃请求数
        features.push(env.core().requests().len() as f32);
        
        // 平均节点利用率
        let avg_utilization: f32 = env.core().nodes().iter()
            .map(|n| n.all_task_cnt() as f32 / 100.0)
            .sum::<f32>() / env.core().nodes().len() as f32;
        features.push(avg_utilization);
        
        // 系统时间（简化）
        features.push(env.core().current_frame() as f32);
        
        features
    }

    /// 使用Score-Rank-Select架构进行调度决策
    fn make_scheduling_decision(&mut self, fn_id: FnId, candidate_nodes: &[NodeId], env: &SimEnvObserve) -> Option<NodeId> {
        if candidate_nodes.is_empty() {
            return None;
        }

        // Step 1: Score - 为每个候选节点计算得分
        let mut node_scores: Vec<(NodeId, f32)> = candidate_nodes.iter()
            .map(|&node_id| {
                let score = self.calculate_node_score(fn_id, node_id, env);
                (node_id, score)
            })
            .collect();

        // Step 2: Rank - 根据得分对节点进行排序
        node_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        // Step 3: Select - 使用强化学习策略选择最终节点
        let selected_node = self.select_node_with_rl(&node_scores, fn_id, env);

        // 记录决策
        if let Some(node_id) = selected_node {
            let state = self.get_current_state_vector(fn_id, env);
            let score = node_scores.iter()
                .find(|(id, _)| *id == node_id)
                .map(|(_, s)| *s)
                .unwrap_or(0.0);
            
            let decision = SchedulingDecision {
                function_id: fn_id,
                selected_node: node_id,
                state,
                score,
                reward: None,
                timestamp: env.core().current_frame() as u64,
            };
            
            self.decision_history.push_back(decision);
            
            // 限制历史记录大小
            if self.decision_history.len() > self.learning_config.replay_buffer_size {
                self.decision_history.pop_front();
            }
        }

        selected_node
    }

    /// 计算节点得分
    fn calculate_node_score(&self, fn_id: FnId, node_id: NodeId, env: &SimEnvObserve) -> f32 {
        let node = &env.core().nodes()[node_id];
        let params = &self.score_rank_select.scoring_params;
        
        let mut score = 0.0;
        
        // CPU利用率得分（越低越好）
        let cpu_utilization = node.all_task_cnt() as f32 / node.cpu as f32;
        score += params.cpu_weight * (1.0 - cpu_utilization.min(1.0));
        
        // 内存利用率得分（越低越好）
        let memory_utilization = node.unready_mem() / node.rsc_limit.mem;
        score += params.memory_weight * (1.0 - memory_utilization);
        
        // 网络延迟得分（简化）
        let network_score = 1.0; // 简化实现
        score += params.network_weight * network_score;
        
        // 冷启动得分
        let has_warm_container = node.fn_containers.borrow().contains_key(&fn_id);
        let cold_start_score = if has_warm_container { 1.0 } else { 0.0 };
        score += params.cold_start_weight * cold_start_score;
        
        // 负载均衡得分
        let load_balance_score = 1.0 / (1.0 + node.all_task_cnt() as f32);
        score += params.load_balance_weight * load_balance_score;
        
        score
    }

    /// 使用强化学习策略选择节点
    fn select_node_with_rl(&mut self, ranked_nodes: &[(NodeId, f32)], fn_id: FnId, env: &SimEnvObserve) -> Option<NodeId> {
        if ranked_nodes.is_empty() {
            return None;
        }

        // 获取当前状态
        let state = self.get_current_state_vector(fn_id, env);
        
        // ε-贪婪策略
        if rand::random::<f32>() < self.rl_agent.epsilon {
            // 探索：随机选择
            let random_index = rand::random::<usize>() % ranked_nodes.len();
            Some(ranked_nodes[random_index].0)
        } else {
            // 利用：使用神经网络预测最佳动作
            let action_values = self.predict_action_values(&state, ranked_nodes);
            let best_action_index = action_values.iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .map(|(i, _)| i)
                .unwrap_or(0);
            
            Some(ranked_nodes[best_action_index].0)
        }
    }

    /// 预测动作价值
    fn predict_action_values(&self, state: &[f32], candidate_nodes: &[(NodeId, f32)]) -> Vec<f32> {
        // 简化的神经网络前向传播
        // 在实际实现中，这里应该是完整的神经网络计算
        candidate_nodes.iter()
            .map(|(_, score)| {
                // 结合状态特征和节点得分
                let state_sum: f32 = state.iter().sum();
                score + state_sum * 0.1
            })
            .collect()
    }

    /// 获取当前状态向量
    fn get_current_state_vector(&self, fn_id: FnId, env: &SimEnvObserve) -> Vec<f32> {
        let mut state = Vec::new();
        
        // 添加函数特征
        if let Some(fn_state) = self.network_state.function_states.get(&fn_id) {
            state.extend_from_slice(fn_state);
        }
        
        // 添加全局特征
        state.extend_from_slice(&self.network_state.global_state);
        
        state
    }

    /// 更新节点性能指标
    fn update_node_metrics(&mut self, node_id: NodeId, node: &crate::node::Node, _env: &SimEnvObserve) {
        let metrics = self.node_metrics.entry(node_id).or_insert_with(|| NodeMetrics {
            cpu_utilization: VecDeque::with_capacity(100),
            memory_utilization: VecDeque::with_capacity(100),
            network_latency: VecDeque::with_capacity(100),
            task_completion_times: VecDeque::with_capacity(100),
            current_tasks: 0,
            avg_response_time: 0.0,
        });
        
        // 更新CPU利用率
        let cpu_util = node.all_task_cnt() as f32 / node.cpu as f32;
        metrics.cpu_utilization.push_back(cpu_util);
        if metrics.cpu_utilization.len() > 100 {
            metrics.cpu_utilization.pop_front();
        }
        
        // 更新内存利用率
        let mem_util = node.unready_mem() / node.rsc_limit.mem;
        metrics.memory_utilization.push_back(mem_util);
        if metrics.memory_utilization.len() > 100 {
            metrics.memory_utilization.pop_front();
        }
        
        // 更新当前任务数
        metrics.current_tasks = node.all_task_cnt();
    }

    /// 更新函数性能指标
    fn update_function_metrics(&mut self, fn_id: FnId, _func: &crate::fn_dag::Func, _env: &SimEnvObserve) {
        let _metrics = self.function_metrics.entry(fn_id).or_insert_with(|| FunctionMetrics {
            execution_times: VecDeque::with_capacity(100),
            cold_starts: 0,
            warm_starts: 0,
            memory_usage: 0.0,
            cpu_usage: 0.0,
            scheduling_frequency: 0.0,
        });
        
        // 这里可以添加更多的函数性能指标更新逻辑
    }

    /// 学习和更新模型
    fn learn_and_update(&mut self) {
        if self.decision_history.len() < self.learning_config.batch_size {
            return;
        }

        // 简化的学习过程
        // 在实际实现中，这里应该包含完整的强化学习算法（如DQN、PPO等）
        
        // 衰减探索率
        self.rl_agent.epsilon = (self.rl_agent.epsilon * self.learning_config.epsilon_decay)
            .max(self.learning_config.min_epsilon);
    }
}

// 实现各种结构的默认值和构造函数
impl NetworkState {
    fn new() -> Self {
        Self {
            node_states: HashMap::new(),
            function_states: HashMap::new(),
            global_state: Vec::new(),
        }
    }
}

impl RLAgent {
    fn new() -> Self {
        Self {
            policy_weights: vec![vec![0.1; 10]; 5], // 简化的网络结构
            value_weights: vec![vec![0.1; 10]; 5],
            learning_rate: 0.001,
            discount_factor: 0.99,
            epsilon: 0.1,
        }
    }
}

impl ScoreRankSelect {
    fn new() -> Self {
        Self {
            scoring_params: ScoringParams {
                cpu_weight: 0.3,
                memory_weight: 0.2,
                network_weight: 0.1,
                cold_start_weight: 0.25,
                load_balance_weight: 0.15,
            },
            ranking_weights: vec![1.0, 0.8, 0.6, 0.4, 0.2],
            selection_params: SelectionParams {
                temperature: 1.0,
                diversity_factor: 0.1,
                greedy_probability: 0.8,
            },
        }
    }
}

impl Default for LearningConfig {
    fn default() -> Self {
        Self {
            batch_size: 32,
            replay_buffer_size: 1000,
            target_update_frequency: 100,
            min_epsilon: 0.01,
            epsilon_decay: 0.995,
            reward_scale: 1.0,
        }
    }
}

// 实现Scheduler trait
impl Scheduler for FaaSRankScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 更新系统状态
        self.update_state(env);
        
        // 遍历所有请求，进行调度
        for (_req_id, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::PreAllDone,
            );

            for fn_id in fns {
                // 获取可用节点
                let candidate_nodes: Vec<NodeId> = env
                    .core().fn_2_nodes()
                    .get(&fn_id)
                    .map(|nodes| nodes.iter().copied().collect())
                    .unwrap_or_default();

                if !candidate_nodes.is_empty() {
                    // 使用FaaSRank算法进行调度决策
                    if let Some(selected_node) = self.make_scheduling_decision(fn_id, &candidate_nodes, env) {
                        log::info!("FaaSRank: schedule fn {} to node {} (score-rank-select)", fn_id, selected_node);
                        
                        // 发送调度命令
                        if let Err(e) = cmd_distributor.send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                            nid: selected_node,
                            reqid: req.req_id,
                            fnid: fn_id,
                            memlimit: None,
                        })) {
                            log::warn!("Failed to send FaaSRank schedule command for fn {} to node {}: {:?}", fn_id, selected_node, e);
                        }
                    }
                }
            }
        }
        
        // 执行学习和模型更新
        self.learn_and_update();
    }
}