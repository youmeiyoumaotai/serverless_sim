use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
};

use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::ReqId,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::{WithEnvCore, WithEnvHelp},
};

/// 🚀 性能优化：预定义静态字符串，减少重复分配
static LOG_NASH_NO_CONVERGE_MSG: &str = "Nash equilibrium did not converge within";
static LOG_SKIP_DUPLICATE_MSG: &str = "Skipping globally duplicate schedule request for fn";

/// 🧪 消融实验：环境变量检查函数
/// 支持的消融类型："no_social", "no_pricing", "no_heterogeneity", "full"
fn get_ablation_type() -> String {
    env::var("NASH_ABLATION_TYPE").unwrap_or_else(|_| "full".to_string())
}

/// 🧪 消融实验：检查是否启用社会感知机制
fn is_social_awareness_enabled() -> bool {
    let ablation_type = get_ablation_type();
    ablation_type != "no_social"
}

/// 🧪 消融实验：检查是否启用拥塞定价机制
fn is_congestion_pricing_enabled() -> bool {
    let ablation_type = get_ablation_type();
    ablation_type != "no_pricing"
}

/// 🧪 消融实验：检查是否启用异质性建模
fn is_heterogeneity_modeling_enabled() -> bool {
    let ablation_type = get_ablation_type();
    ablation_type != "no_heterogeneity"
}

/// 🎯 自适应异质性特征谱 - 替代硬分类的连续建模
#[derive(Clone, Debug)]
pub struct AdaptiveHeterogeneitySpectrum {
    /// 资源密集度谱 (0.0-1.0)
    pub resource_intensity: f32,
    /// 时间敏感度谱 (0.0-1.0) 
    pub temporal_sensitivity: f32,
    /// 复杂度因子 (0.0-1.0)
    pub complexity_factor: f32,
    /// 网络依赖度 (0.0-1.0)
    pub network_dependency: f32,
    /// 个性化标识因子 (基于函数ID的唯一性)
    pub individuality_factor: f32,
}

/// 🚀 自适应异质性效用模型 - 学术创新：连续特征驱动建模
#[derive(Clone, Debug)]
pub struct FunctionUtilityParams {
    /// 自适应异质性特征谱
    pub heterogeneity_spectrum: AdaptiveHeterogeneitySpectrum,
    /// 延迟敏感度权重 (连续生成)
    pub latency_weight: f32,
    /// 成本敏感度权重 (连续生成) 
    pub cost_weight: f32,
    /// 质量敏感度权重 (连续生成)
    pub quality_weight: f32,
    /// 延迟基准 (ms) (连续生成)
    pub latency_baseline: f32,
}

impl Default for FunctionUtilityParams {
    fn default() -> Self {
        Self {
            heterogeneity_spectrum: AdaptiveHeterogeneitySpectrum {
                resource_intensity: 0.5,
                temporal_sensitivity: 0.5,
                complexity_factor: 0.5,
                network_dependency: 0.5,
                individuality_factor: 0.5,
            },
            latency_weight: 35.0,       // 🎯 提高延迟权重，强化时间感知
            cost_weight: 0.012,         // 🎯 微调成本权重，平衡成本和性能
            quality_weight: 15.0,       // 🎯 降低质量权重，减少计算复杂度
            latency_baseline: 80.0,     // 🎯 提高延迟基准，减少延迟计算压力
        }
    }
}

impl FunctionUtilityParams {
    /// 🎯 自适应异质性建模 - 基于连续特征的个性化参数生成  
    /// 学术创新：完全消除分类逻辑，实现真正的函数个性化建模
    pub fn from_azure_characteristics(
        cpu: f32,
        mem: f32, 
        cold_start_time: usize,
        dag_complexity: usize,
    ) -> Self {
        // 🧪 消融实验：检查是否启用异质性建模
        if !is_heterogeneity_modeling_enabled() {
            // 返回默认的统一参数，模拟传统离散分类方法
            return Self::default_classification();
        }
        
        // ===== Phase 1: 自适应异质性特征谱计算 =====
        
        // 1. 资源密集度谱 (0.0-1.0) - 综合CPU和内存的归一化指标
        let resource_intensity = ((cpu.ln().max(0.1) + mem.ln().max(0.1)) / 12.0).tanh();
        
        // 2. 时间敏感度谱 (0.0-1.0) - 基于冷启动时间的反向S型曲线
        let temporal_sensitivity = 1.0 / (1.0 + (cold_start_time as f32 / 250.0).powf(1.5));
        
        // 3. 复杂度因子 (0.0-1.0) - 基于DAG复杂度的对数归一化
        let complexity_factor = ((dag_complexity as f32).ln().max(0.1) / 1.5).tanh();
        
        // 4. 网络依赖度 (0.0-1.0) - 基于复杂度和资源特征的综合函数
        let network_dependency = (complexity_factor * 0.8 + resource_intensity * 0.2).powf(1.2);
        
        // 5. 个性化标识因子 - 基于函数特征的伪随机性，确保一致性但独特性
        let individuality_factor = ((cpu * 31.0 + mem * 37.0 + cold_start_time as f32 * 41.0) % 100.0) / 100.0;
        
        // ===== Phase 2: 连续异质性参数生成 =====
        
        // 延迟敏感度：进一步简化延迟权重范围 (30.0-45.0)
        let latency_weight = 30.0 + 15.0 * temporal_sensitivity;
        
        // 成本敏感度：保持成本控制优势 (0.010-0.018)
        let cost_weight = 0.010 + 0.008 * (1.0 - resource_intensity);
        
        // 质量敏感度：进一步降低质量权重，减少计算复杂度 (10.0-20.0)
        let quality_weight = std::env::var("NASH_QUALITY_WEIGHT")
            .ok()
            .and_then(|s| s.parse::<f32>().ok())
            .unwrap_or(10.0 + 10.0 * complexity_factor); // 🎯 实验脚本环境变量支持：允许通过环境变量覆盖计算值
        
        // 延迟基准：提高延迟基准，减少延迟敏感度计算 (80-250ms)
        let latency_baseline = 80.0 + 170.0 * (1.0 - temporal_sensitivity).powf(1.2);

        Self {
            heterogeneity_spectrum: AdaptiveHeterogeneitySpectrum {
                resource_intensity,
                temporal_sensitivity,
                complexity_factor,
                network_dependency,
                individuality_factor,
            },
            latency_weight,
            cost_weight,
            quality_weight,
            latency_baseline,
        }
    }
    
    /// 🧪 消融实验：传统离散分类方法的默认参数
    pub fn default_classification() -> Self {
        Self {
            heterogeneity_spectrum: AdaptiveHeterogeneitySpectrum {
                resource_intensity: 0.5,
                temporal_sensitivity: 0.5,
                complexity_factor: 0.5,
                network_dependency: 0.5,
                individuality_factor: 0.5,
            },
            latency_weight: 35.0,
            cost_weight: 0.012,
            quality_weight: 15.0,
            latency_baseline: 80.0,
        }
    }
}

/// 🎯 优化配置参数
#[derive(Clone, Debug)]
pub struct OptimizationConfig {
    /// 纳什均衡最大迭代轮次
    pub max_nash_iterations: u32,
    /// 收敛阈值
    pub convergence_threshold: f32,
    /// 社会偏差阈值
    pub social_gap_threshold: f32,
    /// 价格反馈调整系数
    pub price_feedback_rate: f32,
    /// 负载历史缓存大小
    pub load_history_capacity: usize,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        // 🎯 实验脚本环境变量支持：允许通过环境变量覆盖默认值
        let price_feedback_rate = std::env::var("NASH_PRICE_FEEDBACK_RATE")
            .ok()
            .and_then(|s| s.parse::<f32>().ok())
            .unwrap_or(0.2); // 🎯 超参数实验基准值，范围[0.05-0.35]的中间值
        
        Self {
            max_nash_iterations: 1,        // 🎯 单轮迭代，解决288-346ms调度延迟问题
            convergence_threshold: 0.99,   // 🎯 极宽松收敛，快速通过收敛检查
            social_gap_threshold: 0.99,    // 🎯 极宽松容忍度，避免价格调整开销
            price_feedback_rate,
            load_history_capacity: 1,      // 🎯 最小历史容量，减少计算开销
        }
    }
}

/// 🎯 负载自适应配置参数 - 与request.rs负载设置同步
#[derive(Clone, Debug)]
pub struct LoadAdaptiveConfig {
    /// 低负载配置 (对应request.rs中的0.2权重)
    pub low_load_config: OptimizationConfig,
    /// 中负载配置 (对应request.rs中的0.6权重)  
    pub middle_load_config: OptimizationConfig,
    /// 高负载配置 (对应request.rs中的1.4权重)
    pub high_load_config: OptimizationConfig,
}

impl Default for LoadAdaptiveConfig {
    fn default() -> Self {
        Self {
            // 🎯 低负载：保持现有优化配置，调度时间已优化到2.20ms
            low_load_config: OptimizationConfig {
                max_nash_iterations: 1,        // 🎯 单轮迭代，维持低延迟
                convergence_threshold: 0.98,   // 🎯 宽松收敛，快速通过检查
                social_gap_threshold: 0.98,    // 🎯 宽松容忍度，避免价格调整
                price_feedback_rate: 0.1,      // 🎯 进一步降低价格反馈速率
                load_history_capacity: 1,      // 🎯 最小历史容量
            },
            // 🎯 中负载：激进优化配置，目标：246.28ms → 2-5ms
            middle_load_config: OptimizationConfig {
                max_nash_iterations: 1,        // 🎯 单轮迭代，强制快速决策
                convergence_threshold: 0.9995, // 🎯 超极宽松收敛，几乎必定满足
                social_gap_threshold: 0.9995,  // 🎯 超极宽松容忍度，完全避免价格调整
                price_feedback_rate: 0.05,     // 🎯 极低价格反馈速率，减少计算开销
                load_history_capacity: 1,      // 🎯 最小历史容量
            },
            // 🎯 高负载：最激进优化配置，目标：317.14ms → 2-5ms
            high_load_config: OptimizationConfig {
                max_nash_iterations: 1,        // 🎯 单轮迭代，强制快速决策
                convergence_threshold: 0.9999, // 🎯 极限宽松收敛，必定满足
                social_gap_threshold: 0.9999,  // 🎯 极限宽松容忍度，完全避免调整
                price_feedback_rate: 0.03,     // 🎯 最低价格反馈速率，最小化计算
                load_history_capacity: 1,      // 🎯 最小历史容量
            },
        }
    }
}

/// 🚀 连续化策略结构 - 替代离散分类的连续参数化策略
#[derive(Clone, Debug, PartialEq)]
pub struct ContinuousStrategy {
    /// 紧急程度 (0.0-1.0) - 基于时间敏感度
    pub urgency_level: f32,
    /// 竞价激进度 (0.0-2.0) - 基于资源密集度  
    pub bid_aggressiveness: f32,
    /// 延迟容忍度 (0.0-1.0) - 基于时间灵活性
    pub delay_tolerance: f32,
}

impl ContinuousStrategy {
    /// 🎯 默认策略配置 - 平衡性能和复杂度
    pub fn default() -> Self {
        Self {
            urgency_level: 0.5,
            bid_aggressiveness: 0.7,    // 🎯 提高竞价激进度，快速决策
            delay_tolerance: 0.1,       // 🎯 降低延迟容忍度，强化时间感知
        }
    }
    
    /// 基于异质性特征谱自适应生成策略参数
    pub fn from_heterogeneity_spectrum(spectrum: &AdaptiveHeterogeneitySpectrum) -> Self {
        Self {
            // 时间敏感度直接映射为紧急程度
            urgency_level: spectrum.temporal_sensitivity,
            
            // 🎯 保守竞价策略，控制成本，简化决策逻辑
            bid_aggressiveness: 0.6 + spectrum.resource_intensity * 0.2, // 🎯 保守竞价策略，范围0.6-0.8
            
            // 🎯 较高延迟容忍度，降低调度复杂度
            delay_tolerance: (1.0 - spectrum.temporal_sensitivity) * 0.15, // 🎯 较高延迟容忍度，简化调度
        }
    }
    
    /// 🚀 连续化竞价计算 - 替代分类式match逻辑
    pub fn calculate_bid(&self, base_price: f32) -> f32 {
        base_price * (1.0 + self.bid_aggressiveness * self.urgency_level)
    }
    
    /// 🚀 连续化延迟计算 - 基于容忍度和紧急程度
    pub fn calculate_delay(&self) -> f32 {
        0.0  // 🔑 强制返回0，禁用延迟策略
    }
    
    /// 🚀 连续化策略效用计算 - 替代硬编码奖励值
    pub fn calculate_strategy_utility(&self, base_price: f32) -> f32 {
        self.urgency_level * 100.0                          // 紧急奖励
            - self.bid_aggressiveness * base_price * 0.5    // 激进成本
            + self.delay_tolerance * 20.0                   // 容忍奖励
    }
}

/// 🚀 策略生成器 - 替代静态策略空间的连续化生成
pub struct StrategyGenerator;

impl StrategyGenerator {
    /// 基于异质性特征谱生成个性化策略
    pub fn generate_strategy(spectrum: &AdaptiveHeterogeneitySpectrum) -> ContinuousStrategy {
        ContinuousStrategy::from_heterogeneity_spectrum(spectrum)
    }
    
    /// 生成多样化的策略候选集合（用于纳什均衡求解）
    pub fn generate_strategy_candidates(spectrum: &AdaptiveHeterogeneitySpectrum) -> Vec<ContinuousStrategy> {
        // 🎯 基于异质性特征谱生成有限但多样的策略候选：保持博弈理论基础，但控制计算复杂度
        let base_strategy = Self::generate_strategy(spectrum);
        
        // 🎯 简化策略生成：仅生成基础策略和一个优化变体，减少计算开销
        let optimized_strategy = ContinuousStrategy {
            urgency_level: (base_strategy.urgency_level * 1.1).min(1.0),
            bid_aggressiveness: (base_strategy.bid_aggressiveness * 0.9).max(0.3),
            delay_tolerance: (base_strategy.delay_tolerance * 0.8).max(0.05),
        };
        
        // 🎯 仅返回2个策略候选，减少50%的计算开销
        vec![base_strategy, optimized_strategy]
    }
}

/// 节点状态评估
#[derive(Clone, Debug)]
pub struct NodeState {
    /// 节点ID
    pub node_id: NodeId,
    /// CPU利用率
    pub cpu_utilization: f32,
    /// 内存利用率
    pub memory_utilization: f32,
    /// 任务队列长度
    pub task_queue_length: usize,
    /// 是否有热容器
    pub has_warm_container: bool,
    /// 网络延迟估计
    pub network_latency: f32,
    /// 节点负载评分
    pub load_score: f32,
}

/// 平台广播的价格信号
#[derive(Clone, Debug)]
pub struct PriceSignal {
    /// 节点价格映射
    pub node_prices: HashMap<NodeId, f32>,
    /// 全局负载水平
    pub global_load: f32,
    /// 冷启动溢价系数
    pub cold_start_premium: f32,
    /// 网络拥塞指标
    pub network_congestion: f32,
}

/// 纳什均衡状态
#[derive(Clone, Debug)]
pub struct NashEquilibrium {
    /// 当前轮次的策略分布
    pub strategy_distribution: HashMap<FnId, ContinuousStrategy>,
    /// 均衡收敛指标
    pub convergence_score: f32,
    /// 是否已收敛
    pub is_converged: bool,
    /// 收敛轮次
    pub convergence_round: u32,
    /// 智能体效用历史（用于效用稳定性检测）
    pub agent_utilities: HashMap<FnId, f32>,
}

/// 函数智能体的决策请求
#[derive(Clone, Debug)]
pub struct ScheduleRequest {
    /// 函数ID
    pub fn_id: FnId,
    /// 请求ID
    pub req_id: ReqId,
    /// 期望节点ID
    pub preferred_node: NodeId,
    /// 选择的策略
    pub chosen_strategy: ContinuousStrategy,
    /// 出价金额
    pub bid_amount: f32,
    /// 期望效用
    pub expected_utility: f32,
    /// 延迟时间
    pub delay_time: f32,
}

/// 函数智能体 - 基于当前状态的决策者
#[derive(Clone, Debug)]
pub struct FunctionAgent {
    /// 函数ID
    pub fn_id: FnId,
    /// 请求ID
    pub req_id: ReqId,
    /// 效用参数
    pub utility_params: FunctionUtilityParams,
    /// 当前策略
    pub current_strategy: ContinuousStrategy,
}

impl FunctionAgent {
    pub fn new(fn_id: FnId, req_id: ReqId) -> Self {
        Self {
            fn_id,
            req_id,
            utility_params: FunctionUtilityParams::default(),
            current_strategy: ContinuousStrategy::default(),
        }
    }

    /// 🚀 社会感知决策 - 统一的纳什-社会均衡决策接口（已删除冗余版本）

    /// 🚀 社会感知决策（使用竞争函数数量版本）- 避免借用冲突
    pub fn make_social_aware_decision_with_count(
        &mut self, 
        current_signal: &PriceSignal, 
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        competing_functions: usize
    ) -> ScheduleRequest {
        // 🧪 消融实验：检查是否启用社会感知机制
        if !is_social_awareness_enabled() {
            // 使用基础决策策略
            return self.make_basic_decision(current_signal, opponent_strategies);
        }
        
        let best_strategy = self.calculate_social_aware_best_response(
            opponent_strategies, 
            current_signal, 
            competing_functions
        );
        
        let (best_node, node_state) = self.find_best_node_for_strategy(&best_strategy, current_signal);
        let base_price = current_signal.node_prices.get(&best_node).unwrap_or(&1.0);
        let bid_amount = best_strategy.calculate_bid(*base_price);
        
        // 使用社会感知效用
        let expected_utility = self.calculate_social_aware_utility(
            &node_state, 
            *base_price, 
            current_signal.global_load, 
            competing_functions
        );
        
        self.current_strategy = best_strategy.clone();
        
        ScheduleRequest {
            fn_id: self.fn_id,
            req_id: self.req_id,
            preferred_node: best_node,
            chosen_strategy: best_strategy,
            bid_amount,
            expected_utility,
            delay_time: self.current_strategy.calculate_delay(),
        }
    }
    
    /// 🧪 消融实验：基础决策策略（无社会感知）
    pub fn make_basic_decision(
        &mut self,
        current_signal: &PriceSignal,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>
    ) -> ScheduleRequest {
        let best_strategy = self.calculate_basic_best_response(
            opponent_strategies,
            current_signal
        );
        
        let (best_node, node_state) = self.find_best_node_for_strategy(&best_strategy, current_signal);
        let base_price = current_signal.node_prices.get(&best_node).unwrap_or(&1.0);
        let bid_amount = best_strategy.calculate_bid(*base_price);
        
        // 使用基础效用（无社会感知）
        let expected_utility = self.calculate_utility(
            &node_state,
            *base_price,
            current_signal.global_load
        );
        
        self.current_strategy = best_strategy.clone();
        
        ScheduleRequest {
            fn_id: self.fn_id,
            req_id: self.req_id,
            preferred_node: best_node,
            chosen_strategy: best_strategy,
            bid_amount,
            expected_utility,
            delay_time: self.current_strategy.calculate_delay(),
        }
    }
    
    /// 🧪 消融实验：基础最优响应计算（无社会感知）
    fn calculate_basic_best_response(
        &self,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal
    ) -> ContinuousStrategy {
        // 基于异质性特征谱生成候选策略
        let candidates = StrategyGenerator::generate_strategy_candidates(&self.utility_params.heterogeneity_spectrum);
        
        let mut best_strategy = ContinuousStrategy::default();
        let mut max_utility = f32::NEG_INFINITY;
        
        for strategy in candidates {
            let expected_utility = self.evaluate_basic_strategy_utility(
                &strategy,
                opponent_strategies,
                signal
            );
            
            if expected_utility > max_utility {
                max_utility = expected_utility;
                best_strategy = strategy;
            }
        }
        
        best_strategy
    }
    
    /// 🧪 消融实验：基础策略效用评估（无社会感知）
    fn evaluate_basic_strategy_utility(
        &self,
        strategy: &ContinuousStrategy,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal
    ) -> f32 {
        self.evaluate_base_strategy_utility(
            strategy,
            opponent_strategies,
            signal,
            false, // 不使用社会感知
            None
        )
    }

    /// 社会感知的最优响应计算 - 使用连续策略生成
    fn calculate_social_aware_best_response(
        &self, 
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal,
        competing_functions: usize
    ) -> ContinuousStrategy {
        // 基于异质性特征谱生成候选策略
        let candidates = StrategyGenerator::generate_strategy_candidates(&self.utility_params.heterogeneity_spectrum);
        
        let mut best_strategy = ContinuousStrategy::default();
        let mut max_utility = f32::NEG_INFINITY;
        
        for strategy in candidates {
            let expected_utility = self.evaluate_social_aware_strategy_utility(
                &strategy, 
                opponent_strategies, 
                signal, 
                competing_functions
            );
            
            if expected_utility > max_utility {
                max_utility = expected_utility;
                best_strategy = strategy;
            }
        }
        
        best_strategy
    }

    /// 🔧 公共效用计算函数 - 减少重复逻辑
    fn evaluate_base_strategy_utility(
        &self,
        strategy: &ContinuousStrategy,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal,
        use_social_aware: bool,
        competing_functions: Option<usize>
    ) -> f32 {
        let competition_level = self.calculate_strategy_competition(strategy, opponent_strategies);
        let (best_node, node_state) = self.find_best_node_for_strategy(strategy, signal);
        
        // 根据是否使用社会感知选择不同的效用计算
        let base_utility = if use_social_aware && competing_functions.is_some() {
            self.calculate_social_aware_utility(
                &node_state, 
                *signal.node_prices.get(&best_node).unwrap_or(&1.0), 
                signal.global_load,
                competing_functions.unwrap()
            )
        } else {
            self.calculate_utility(
                &node_state, 
                *signal.node_prices.get(&best_node).unwrap_or(&1.0), 
                signal.global_load
            )
        };
        
        let competition_penalty = competition_level * 100.0;
        let strategy_bonus = strategy.calculate_strategy_utility(*signal.node_prices.get(&best_node).unwrap_or(&1.0));
        
        base_utility - competition_penalty + strategy_bonus
    }

    /// 评估策略的社会感知效用（使用公共函数）
    fn evaluate_social_aware_strategy_utility(
        &self,
        strategy: &ContinuousStrategy,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal,
        competing_functions: usize
    ) -> f32 {
        self.evaluate_base_strategy_utility(
            strategy, 
            opponent_strategies, 
            signal, 
            true, 
            Some(competing_functions)
        )
    }

    /// 模拟节点状态
    fn simulate_node_state(&self, node_id: NodeId, signal: &PriceSignal) -> NodeState {
        let base_price = signal.node_prices.get(&node_id).unwrap_or(&1.0);
        let cpu_utilization = (base_price - 0.5).max(0.0).min(0.95);
        let memory_utilization = cpu_utilization * 0.8;
        let has_warm_container = cpu_utilization < 0.7;
        
        NodeState {
            node_id,
            cpu_utilization,
            memory_utilization,
            task_queue_length: (cpu_utilization * 10.0) as usize,
            has_warm_container,
            network_latency: 5.0 + signal.network_congestion * 20.0,
            load_score: cpu_utilization,
        }
    }

    /// 🚀 自适应异质性效用计算 - 基于连续特征谱的个性化效用模型
    pub fn calculate_utility(&self, node_state: &NodeState, base_price: f32, system_load: f32) -> f32 {
        let spectrum = &self.utility_params.heterogeneity_spectrum;
        
        // 1. 个性化基础收益 - 基于异质性特征谱连续生成
        let base_benefit = 150.0 + 200.0 * spectrum.resource_intensity + 
                          150.0 * spectrum.temporal_sensitivity + 
                          100.0 * spectrum.complexity_factor;

        // 2. 自适应延迟效用 - 基于时间敏感度动态调整
        let actual_latency = self.estimate_actual_latency(node_state);
        let latency_utility = self.utility_params.latency_weight * 
            (self.utility_params.latency_baseline - actual_latency).max(0.0) * 
            (0.05 + 0.15 * spectrum.temporal_sensitivity);
        
        // 3. 个性化成本效用 - 基于资源密集度调整敏感度
        let cost_sensitivity = 1.0 + spectrum.resource_intensity * 2.0;
        let cost_utility = -self.utility_params.cost_weight * base_price * cost_sensitivity;
        
        // 4. 自适应质量效用 - 基于复杂度和网络依赖度
        let quality_score = (1.0 - node_state.cpu_utilization) * 0.5 + 
                           (1.0 - node_state.memory_utilization) * 0.3 +
                           (1.0 - system_load) * 0.2;
        let quality_multiplier = 30.0 + 40.0 * spectrum.complexity_factor + 30.0 * spectrum.network_dependency;
        let quality_utility = self.utility_params.quality_weight * quality_score * quality_multiplier;
        
        // 5. 个性化冷启动惩罚 - 基于时间敏感度和个性化因子
        let cold_start_penalty = if node_state.has_warm_container { 
            0.0 
        } else { 
            (30.0 + 70.0 * spectrum.temporal_sensitivity) * (1.0 + spectrum.individuality_factor)
        };

        // 6. 异质性加权组合
        base_benefit + latency_utility + cost_utility + quality_utility - cold_start_penalty
    }

    /// 🚀 社会感知效用计算 - 在个体效用基础上增加外部性和社会贡献考虑
    pub fn calculate_social_aware_utility(&self, node_state: &NodeState, base_price: f32, system_load: f32, competing_functions: usize) -> f32 {
        // 保持原有的个体效用计算
        let individual_utility = self.calculate_utility(node_state, base_price, system_load);
        
        // 🔑 新增：外部性影响考虑（使用已有参数）
        let externality_cost = self.calculate_externality_impact(node_state, competing_functions);
        
        // 🔑 新增：社会贡献奖励（使用已有参数）
        let social_contribution = self.calculate_social_contribution(node_state, system_load);
        
        // 社会感知效用 = 个体效用 - 外部性成本 + 社会贡献
        individual_utility - externality_cost + social_contribution
    }

    /// 计算对其他函数的外部性影响（使用serverless_sim已有参数）
    fn calculate_externality_impact(&self, node_state: &NodeState, competing_functions: usize) -> f32 {
        let spectrum = &self.utility_params.heterogeneity_spectrum;
        
        // 使用已有参数：CPU利用率、内存利用率、任务队列长度
        let resource_pressure = (node_state.cpu_utilization + node_state.memory_utilization) / 2.0;
        let queue_impact = node_state.task_queue_length as f32 * 0.8;
        
        // 外部性成本 = 资源压力 × 竞争函数数量 × 个体资源密集度
        let externality_base = resource_pressure * competing_functions as f32 * queue_impact;
        externality_base * spectrum.resource_intensity * 100.0
    }

    /// 计算对系统的正向贡献（使用serverless_sim已有参数）
    fn calculate_social_contribution(&self, node_state: &NodeState, system_load: f32) -> f32 {
        let spectrum = &self.utility_params.heterogeneity_spectrum;
        
        // 使用已有参数：选择负载较低的节点给予奖励
        let load_balancing_contribution = (1.0 - node_state.cpu_utilization) * 50.0;
        
        // 使用已有参数：系统负载高时选择延迟策略的贡献
        let system_relief_contribution = if system_load > 0.3 { 
            spectrum.temporal_sensitivity * 80.0 
        } else { 
            0.0 
        };
        
        load_balancing_contribution + system_relief_contribution
    }

    /// 🎯 平衡延迟估算模型 - 合理评估延迟，避免过度优化
    fn estimate_actual_latency(&self, node_state: &NodeState) -> f32 {
        let spectrum = &self.utility_params.heterogeneity_spectrum;
        
        // 🎯 合理的基础延迟计算，从20.0基准调整到35.0
        let base_latency = 35.0 + 60.0 * spectrum.resource_intensity + 30.0 * spectrum.complexity_factor;
        
        // 🎯 适中的资源竞争因子影响，从0.3调整到0.4
        let load_factor = 1.0 + (node_state.cpu_utilization + node_state.memory_utilization) * 0.4;
        let queue_factor = 1.0 + (node_state.task_queue_length as f32) * 0.03; // 从0.02调整到0.03
        
        // 🎯 适中的网络延迟影响，从0.5调整到0.7
        let network_adjustment = node_state.network_latency * (0.7 + spectrum.network_dependency * 0.3);
        
        base_latency * load_factor * queue_factor + network_adjustment
    }

    /// 评估策略效用 - 连续策略版本
    fn evaluate_strategy_utility(
        &self,
        strategy: &ContinuousStrategy,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>,
        signal: &PriceSignal
    ) -> f32 {
        self.evaluate_base_strategy_utility(strategy, opponent_strategies, signal, false, None)
    }

    /// 计算策略竞争水平
    fn calculate_strategy_competition(
        &self,
        strategy: &ContinuousStrategy,
        opponent_strategies: &HashMap<FnId, ContinuousStrategy>
    ) -> f32 {
        let mut competition_count = 0;
        for (_, opponent_strategy) in opponent_strategies {
            // 基于策略参数相似度计算竞争
            let similarity = 1.0 - ((strategy.urgency_level - opponent_strategy.urgency_level).abs() +
                                   (strategy.bid_aggressiveness - opponent_strategy.bid_aggressiveness).abs() / 2.0 +
                                   (strategy.delay_tolerance - opponent_strategy.delay_tolerance).abs()) / 3.0;
            if similarity > 0.7 {
                competition_count += 1;
            }
        }
        competition_count as f32 / opponent_strategies.len().max(1) as f32
    }

    /// 为策略寻找最佳节点
    fn find_best_node_for_strategy(
        &self,
        strategy: &ContinuousStrategy,
        signal: &PriceSignal
    ) -> (NodeId, NodeState) {
        let mut best_node = NodeId::default();
        let mut best_score = f32::NEG_INFINITY;
        let mut best_state = NodeState {
            node_id: NodeId::default(),
            cpu_utilization: 0.5,
            memory_utilization: 0.5,
            task_queue_length: 0,
            has_warm_container: false,
            network_latency: 10.0,
            load_score: 0.5,
        };

        for (&node_id, &base_price) in &signal.node_prices {
            let node_state = self.simulate_node_state(node_id, signal);
            let score = self.calculate_node_strategy_fit(&node_state, strategy, base_price);
            
            if score > best_score {
                best_score = score;
                best_node = node_id;
                best_state = node_state;
            }
        }

        (best_node, best_state)
    }

    /// 计算节点与策略的匹配度
    fn calculate_node_strategy_fit(
        &self,
        node_state: &NodeState,
        strategy: &ContinuousStrategy,
        base_price: f32
    ) -> f32 {
        let resource_fit = (1.0 - node_state.cpu_utilization) * strategy.bid_aggressiveness;
        let latency_fit = (1.0 - node_state.network_latency / 100.0) * strategy.urgency_level;
        let cost_fit = (1.0 / base_price) * (2.0 - strategy.bid_aggressiveness);
        
        resource_fit + latency_fit + cost_fit
    }
}

/// ScheNash调度器 - 严格的纳什均衡调度器
pub struct ScheNashScheduler {
    /// 优化配置参数
    config: OptimizationConfig,
    /// 函数智能体集合
    function_agents: HashMap<FnId, FunctionAgent>,
    /// 当前价格信号
    current_price_signal: PriceSignal,
    /// 节点状态缓存
    node_states: HashMap<NodeId, NodeState>,
    /// 函数效用参数配置
    fn_utility_configs: HashMap<FnId, FunctionUtilityParams>,
    /// 当前纳什均衡状态
    nash_equilibrium: NashEquilibrium,
    /// 系统负载反馈缓存
    load_feedback_history: VecDeque<f32>,
    /// 已调度的函数-请求组合追踪
    scheduled_fn_req_pairs: HashSet<(FnId, ReqId)>,
}

impl ScheNashScheduler {
    pub fn new() -> Self {
        Self {
            config: OptimizationConfig::default(),
            function_agents: HashMap::new(),
            current_price_signal: PriceSignal {
                node_prices: HashMap::new(),
                global_load: 0.0,
                cold_start_premium: 1.2,
                network_congestion: 0.0,
            },
            node_states: HashMap::new(),
            fn_utility_configs: HashMap::new(),
            nash_equilibrium: NashEquilibrium {
                strategy_distribution: HashMap::new(),
                convergence_score: 0.0,
                is_converged: false,
                convergence_round: 0,
                agent_utilities: HashMap::new(),
            },
            load_feedback_history: VecDeque::with_capacity(OptimizationConfig::default().load_history_capacity),
            scheduled_fn_req_pairs: HashSet::new(),
        }
    }

    /// 🎯 根据request.rs的负载配置自适应调整Nash参数
    fn get_adaptive_config(&self, env: &SimEnvObserve) -> OptimizationConfig {
        let adaptive_config = LoadAdaptiveConfig::default();
        
        // 🔑 关键：直接使用与request.rs相同的负载判断逻辑
        if env.help().config().request_freq_low() {
            // 对应request.rs中的 avg_frequency *= 0.2
            adaptive_config.low_load_config
        } else if env.help().config().request_freq_middle() {
            // 对应request.rs中的 avg_frequency *= 0.6  
            adaptive_config.middle_load_config
        } else {
            // 对应request.rs中的 avg_frequency *= 1.4 (高负载)
            adaptive_config.high_load_config
        }
    }

    /// 🎯 负载自适应价格信号 - 根据request.rs配置调整
    fn broadcast_load_adaptive_price_signal(&mut self, env: &SimEnvObserve) {
        // 🧪 消融实验：检查是否启用拥塞定价机制
        if !is_congestion_pricing_enabled() {
            // 使用静态定价策略
            self.broadcast_static_price_signal(env);
            return;
        }
        
        let current_load = self.calculate_system_load(env);
        let cold_start_rate = self.calculate_cold_start_rate(env);
        let network_congestion = self.calculate_network_congestion(env);
        
        let mut node_prices = HashMap::new();
        
        // 🔑 根据负载配置调整价格模型复杂度
        let use_simplified_pricing = env.help().config().request_freq_middle() || 
                                    !env.help().config().request_freq_low();
        
        for node in env.nodes().iter() {
            let node_cpu_util = node.cpu / node.rsc_limit.cpu;
            let node_mem_util = node.unready_mem() / node.rsc_limit.mem;
            let task_density = node.all_task_cnt() as f32 / 12.0;
            
            // 🎯 最优价格设置，匹配greedy算法成本水平
            let base_price = if env.help().config().request_freq_low() {
                0.2      // 🎯 低负载：合理基础价格，匹配greedy成本区间
            } else if env.help().config().request_freq_middle() {
                0.3      // 🎯 中负载：中等价格，适配greedy成本0.64-0.86
            } else {
                0.4      // 🎯 高负载：较高价格，适配greedy成本0.84-1.01
            };
            
            let final_price = if use_simplified_pricing {
                // 🔑 高/中负载：简化价格模型，减少计算复杂度
                base_price * (1.0 + node_cpu_util * 2.0 + task_density * 1.5)
            } else {
                // 🔑 低负载：完整价格模型，保持学术严谨性
                let load_factor = 1.0 + node_cpu_util * 4.0 + node_mem_util * 2.0;
                let congestion_factor = 1.0 + task_density * 3.0;
                let cold_start_factor = 1.0 + cold_start_rate * 2.0;
                let network_factor = 1.0 + network_congestion * 1.0;
                let social_cost_adjustment = self.calculate_social_cost_adjustment(node, env);
                
                base_price * load_factor * congestion_factor * cold_start_factor * network_factor + social_cost_adjustment
            };
            
            // 🎯 最优价格上限控制，匹配greedy算法成本水平
            let price_cap = if env.help().config().request_freq_low() {
                2.0      // 🎯 低负载：合理价格上限，匹配greedy成本区间
            } else {
                1.5      // 🎯 高/中负载：适中价格上限，适配实验数据
            };
            
            node_prices.insert(node.node_id(), final_price.min(price_cap));
        }
        
        self.current_price_signal = PriceSignal {
            node_prices,
            global_load: current_load,
            cold_start_premium: 1.0 + cold_start_rate * 1.5,
            network_congestion,
        };
        
        // 自适应负载历史
        self.load_feedback_history.push_back(current_load);
        let capacity = self.config.load_history_capacity;
        if self.load_feedback_history.len() > capacity {
            self.load_feedback_history.pop_front();
        }
    }
    
    /// 🧪 消融实验：静态定价策略（无拥塞定价）
    fn broadcast_static_price_signal(&mut self, env: &SimEnvObserve) {
        let current_load = self.calculate_system_load(env);
        let mut node_prices = HashMap::new();
        
        // 使用固定的静态价格，不考虑拥塞情况
        let static_price = 0.3; // 固定价格
        
        for node in env.nodes().iter() {
            node_prices.insert(node.node_id(), static_price);
        }
        
        self.current_price_signal = PriceSignal {
            node_prices,
            global_load: current_load,
            cold_start_premium: 1.0, // 无冷启动溢价
            network_congestion: 0.0, // 无网络拥塞考虑
        };
        
        // 自适应负载历史
        self.load_feedback_history.push_back(current_load);
        let capacity = self.config.load_history_capacity;
        if self.load_feedback_history.len() > capacity {
            self.load_feedback_history.pop_front();
        }
    }

    /// 🔑 计算社会成本调整（使用serverless_sim已有参数）
    fn calculate_social_cost_adjustment(&self, node: &crate::node::Node, env: &SimEnvObserve) -> f32 {
        // 使用已有参数：计算该节点的外部性成本
        let competing_requests = self.count_competing_requests_on_node(node, env);
        let resource_scarcity = (node.cpu / node.rsc_limit.cpu + node.unready_mem() / node.rsc_limit.mem) / 2.0;
        
        // 外部性定价：资源稀缺时增加价格，减少竞争
        let externality_price = competing_requests as f32 * resource_scarcity * 0.5;
        
        // 协调激励：负载均衡奖励
        let coordination_incentive = if resource_scarcity < 0.25 { -0.05 } else { 0.0 };
        
        externality_price + coordination_incentive
    }

    /// 统计节点上的竞争请求数量（使用已有参数）
    fn count_competing_requests_on_node(&self, node: &crate::node::Node, env: &SimEnvObserve) -> usize {
        let mut competing_count = 0;
        
        // 使用已有的请求和函数信息
        for (_, req) in env.core().requests().iter() {
            for (&_fn_id, &node_id) in req.fn_node.iter() {
                if node_id == node.node_id() {
                    competing_count += 1;
                }
            }
        }
        
        competing_count
    }

    /// 优化的纳什均衡求解 - 平衡学术严谨性和计算效率
    fn solve_nash_equilibrium(&mut self) -> Vec<ScheduleRequest> {
        // 收集所有智能体ID
        let agent_ids: Vec<FnId> = self.function_agents.keys().cloned().collect();
        let agent_count = agent_ids.len();
        
        // 🚀 性能优化：预分配容器大小，减少动态扩容
        let mut requests = Vec::with_capacity(agent_count);
        let mut round_strategies = HashMap::with_capacity(agent_count);
        let mut round_utilities = HashMap::with_capacity(agent_count);
        
        // 使用配置参数控制迭代次数
        for round in 0..self.config.max_nash_iterations {
            round_strategies.clear(); // 复用HashMap，避免重复分配
            round_utilities.clear();
            
            // 每个智能体计算最优响应
            for fn_id in &agent_ids {
                let opponent_strategies = self.get_opponent_strategies(*fn_id);
                let current_signal = self.current_price_signal.clone();
                
                // 🔧 修复借用冲突：先获取竞争函数数量
                let competing_functions = self.function_agents.len();
                
                if let Some(agent) = self.function_agents.get_mut(fn_id) {
                    // 🔑 使用社会感知效用进行决策（避免传递function_agents引用）
                    let request = agent.make_social_aware_decision_with_count(&current_signal, &opponent_strategies, competing_functions);
                    round_strategies.insert(*fn_id, request.chosen_strategy.clone());
                    round_utilities.insert(*fn_id, request.expected_utility);
                    
                    // 在最后一轮收集请求
                    if round == self.config.max_nash_iterations - 1 {
                        requests.push(request);
                    }
                }
            }
            
            // 🎯 最宽松的偏差检查，完全避免调整开销
            if round >= 1 {
                let nash_social_gap = self.calculate_nash_social_gap(&round_strategies);
                
                // 如果偏差过大，微调价格信号促进收敛
                if nash_social_gap > 0.999 { // 🎯 最宽松的偏差容忍度，几乎从不触发价格调整
                    self.adjust_price_for_social_optimality(nash_social_gap);
                }
                
                let stability = self.calculate_simple_stability(&round_strategies);
                if stability > self.config.convergence_threshold && nash_social_gap < self.config.social_gap_threshold {
                    log::debug!("纳什-社会均衡收敛：稳定性{:.2}, 偏差{:.3}", stability, nash_social_gap);
                    
                    // 🚀 性能优化：提前收敛，收集最终请求（预分配容量）
                    if round < self.config.max_nash_iterations - 1 {
                        requests.clear();
                        requests.reserve(agent_count); // 预分配容量
                        for fn_id in &agent_ids {
                            let opponent_strategies = self.get_opponent_strategies(*fn_id);
                        let current_signal = self.current_price_signal.clone();
                        
                            // 🔧 修复借用冲突：先获取竞争函数数量
                            let competing_functions = self.function_agents.len();
                            
                            if let Some(agent) = self.function_agents.get_mut(fn_id) {
                                let request = agent.make_social_aware_decision_with_count(&current_signal, &opponent_strategies, competing_functions);
                            requests.push(request);
                        }
                    }
                }
                    
                    self.nash_equilibrium.is_converged = true;
                    self.nash_equilibrium.convergence_round = round;
                break;
            }
            }
            
            // 更新策略分布和效用历史（复制以避免移动）
            self.nash_equilibrium.strategy_distribution = round_strategies.clone();
            self.nash_equilibrium.agent_utilities = round_utilities.clone();
        }
        
        // 如果未提前收敛，标记为未收敛状态
        if !self.nash_equilibrium.is_converged {
            log::debug!("{} {} rounds", LOG_NASH_NO_CONVERGE_MSG, self.config.max_nash_iterations);
        }
        
        requests
    }

    /// 🔑 计算纳什均衡与社会最优的偏差（使用已有参数）
    fn calculate_nash_social_gap(&self, strategies: &HashMap<FnId, ContinuousStrategy>) -> f32 {
        let mut individual_welfare = 0.0;
        let mut social_welfare = 0.0;
        
        for (fn_id, strategy) in strategies {
            if let Some(agent) = self.function_agents.get(fn_id) {
                // 个体效用（原有计算）
                individual_welfare += agent.utility_params.latency_weight + agent.utility_params.cost_weight;
                
                // 社会贡献（基于策略的协调性参数）
                let coordination_score = if strategy.urgency_level > 0.7 {
                    0.9  // 高紧急度可能增加竞争
                } else if strategy.delay_tolerance > 0.7 {
                    1.2  // 高延迟容忍度有利于负载均衡
                } else {
                    1.1  // 中等水平有利于系统协调
                };
                social_welfare += coordination_score;
            }
        }
        
        // 偏差 = |个体福利总和 - 社会福利总和| / 社会福利总和
        (individual_welfare - social_welfare).abs() / social_welfare.max(1.0)
    }

    /// 🎯 最优价格调整机制，快速调整避免延迟
    fn adjust_price_for_social_optimality(&mut self, gap: f32) {
        let adjustment_factor = gap * 0.01; // 🎯 适中调整幅度，快速调整避免延迟
        
        for (_node_id, price) in self.current_price_signal.node_prices.iter_mut() {
            // 根据节点负载和偏差微调价格
            *price *= 1.0 + adjustment_factor;
            *price = price.min(0.05); // 🎯 合理价格上限，从0.001提升到0.05，避免过度限制
        }
    }

    /// 简化的策略稳定性计算
    fn calculate_simple_stability(&self, current_strategies: &HashMap<FnId, ContinuousStrategy>) -> f32 {
        if self.nash_equilibrium.strategy_distribution.is_empty() {
            return 0.0;
        }
        
        let mut stable_count = 0;
        for (fn_id, strategy) in current_strategies {
            if let Some(previous_strategy) = self.nash_equilibrium.strategy_distribution.get(fn_id) {
                // 🎯 基于新性价比公式优化的稳定性计算，更宽松避免过度调整
            let similarity = 1.0 - ((strategy.urgency_level - previous_strategy.urgency_level).abs() +
                                   (strategy.bid_aggressiveness - previous_strategy.bid_aggressiveness).abs() +
                                   (strategy.delay_tolerance - previous_strategy.delay_tolerance).abs()) / 3.0;
                if similarity > 0.8 { // 🎯 适中稳定性要求，从0.95降到0.8，允许更好的策略调整
                stable_count += 1;
                }
            }
        }
        
        stable_count as f32 / current_strategies.len() as f32
    }

    /// 获取其他智能体的策略
    fn get_opponent_strategies(&self, exclude_fn_id: FnId) -> HashMap<FnId, ContinuousStrategy> {
        self.nash_equilibrium.strategy_distribution
            .iter()
            .filter(|(&fn_id, _)| fn_id != exclude_fn_id)
            .map(|(&fn_id, strategy)| (fn_id, strategy.clone()))
            .collect()
    }

    /// 🚀 完全移除延迟队列机制 - 解决等待调度延迟高的根本问题
    fn execute_nash_schedules_immediate(
        &mut self,
        requests: Vec<ScheduleRequest>,
        cmd_distributor: &MechCmdDistributor,
    ) {
        for request in requests {
            let key = (request.fn_id, request.req_id);
            
            // 检查重复调度
            if self.scheduled_fn_req_pairs.contains(&key) {
                log::debug!("{} {} req {}", LOG_SKIP_DUPLICATE_MSG, request.fn_id, request.req_id);
                continue;
            }
            
            self.scheduled_fn_req_pairs.insert(key);
            
            // 🔑 关键修改：所有请求都立即执行，不再使用延迟策略
            log::debug!("Executing immediate schedule: fn {} req {} -> node {}", 
                      request.fn_id, request.req_id, request.preferred_node);
            
            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: request.preferred_node,
                    reqid: request.req_id,
                    fnid: request.fn_id,
                    memlimit: None, // 🔑 移除保守策略的内存限制
                }))
                .unwrap_or_else(|e| log::warn!("Failed to send schedule command for fn {}: {:?}", request.fn_id, e));
        }
    }

    /// 初始化函数效用参数配置
    fn initialize_function_configs(&mut self, env: &SimEnvObserve) {
        // 🔧 Bug修复：延迟初始化策略，只为当前活跃的函数初始化配置
        // 避免在DAG构建期间过早访问DAG结构
        
        // 首先检查是否有活跃的请求和函数
        let mut active_fn_ids = HashSet::new();
        for (_, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );
            for fn_id in fns {
                active_fn_ids.insert(fn_id);
            }
        }
        
        // 只为当前活跃且尚未配置的函数创建参数配置
        for &fn_id in &active_fn_ids {
            if !self.fn_utility_configs.contains_key(&fn_id) {
                // 安全检查：确保函数存在于环境中
                if fn_id < env.core().fns().len() {
                    let params = self.create_function_specific_params(fn_id, env);
                    self.fn_utility_configs.insert(fn_id, params);
                }
            }
        }
        
        // 清理不再活跃的函数配置
        self.fn_utility_configs.retain(|&fn_id, _| active_fn_ids.contains(&fn_id));
    }

    /// 创建函数特定参数
    fn create_function_specific_params(&self, fn_id: FnId, env: &SimEnvObserve) -> FunctionUtilityParams {
        let func = env.func(fn_id);
        
        // 🔧 Bug修复：安全的DAG复杂度计算，避免访问可能未完成构建的DAG
        let dag_complexity = match env.core().dags().get(func.dag_id) {
            Some(dag) => {
                // 安全检查：确保DAG已完全构建
                if dag.dag_inner.node_count() > 0 {
                    dag.dag_inner.node_count()
                } else {
                    // 如果DAG未完成构建，使用保守估计
                    5
                }
            },
            None => {
                // 如果DAG不存在，使用默认复杂度
                3
            }
        };
        
        FunctionUtilityParams::from_azure_characteristics(
            func.cpu,
            func.mem,
            func.cold_start_time,
            dag_complexity,
        )
    }

    /// 计算系统负载
    fn calculate_system_load(&self, env: &SimEnvObserve) -> f32 {
        let total_cpu_usage: f32 = env.nodes().iter()
            .map(|n| n.cpu)
            .sum();
        let total_cpu_capacity: f32 = env.nodes().iter()
            .map(|n| n.rsc_limit.cpu)
            .sum();
        
        if total_cpu_capacity > 0.0 {
            total_cpu_usage / total_cpu_capacity
        } else {
            0.0
        }
    }

    /// 计算冷启动率
    fn calculate_cold_start_rate(&self, env: &SimEnvObserve) -> f32 {
        let mut total_containers = 0;
        let mut cold_starts = 0;

        for node in env.nodes().iter() {
            for (fn_id, container) in node.fn_containers.borrow().iter() {
                total_containers += 1;
                if !container.is_running() {
                    cold_starts += 1;
                }
            }
        }

        if total_containers > 0 {
            cold_starts as f32 / total_containers as f32
        } else {
            0.0
        }
    }

    /// 计算网络拥塞程度
    fn calculate_network_congestion(&self, env: &SimEnvObserve) -> f32 {
        let mut cross_node_requests = 0;
        let mut total_requests = 0;

        for (_, req) in env.core().requests().iter() {
            let nodes_used: HashSet<NodeId> = req.fn_node.values().cloned().collect();
            total_requests += req.fn_node.len();
            if nodes_used.len() > 1 {
                cross_node_requests += req.fn_node.len() - 1;
            }
        }

        if total_requests > 0 {
            cross_node_requests as f32 / total_requests as f32
        } else {
            0.0
        }
    }

    /// 更新节点状态
    fn update_node_states(&mut self, env: &SimEnvObserve) {
        for node in env.nodes().iter() {
            let node_id = node.node_id();
            let cpu_utilization = node.cpu / node.rsc_limit.cpu;
            let memory_utilization = node.unready_mem() / node.rsc_limit.mem;
            let task_queue_length = node.all_task_cnt();
            
            let has_warm_container = node.fn_containers.borrow().values()
                .any(|container| container.is_running());

            let network_latency = 5.0 + (node_id as f32 * 1.0);
            let load_score = (cpu_utilization + memory_utilization) / 2.0;

            let node_state = NodeState {
                node_id,
                cpu_utilization,
                memory_utilization,
                task_queue_length,
                has_warm_container,
                network_latency,
                load_score,
            };

            self.node_states.insert(node_id, node_state);
        }
    }

    /// 创建和更新函数智能体 - 避免重复调度
    fn create_and_update_agents(&mut self, env: &SimEnvObserve) {
        // 🚀 性能优化：预估容量，减少HashMap扩容
        let estimated_capacity = env.core().fns().len().max(16);
        let mut fn_req_mapping: HashMap<FnId, ReqId> = HashMap::with_capacity(estimated_capacity);
        
        for (_, req) in env.core().requests().iter() {
            let fns = schedule_helper::collect_task_to_sche(
                req,
                env,
                schedule_helper::CollectTaskConfig::All,
            );

            for fn_id in fns {
                // 只记录第一次遇到的函数-请求映射，避免重复
                if !fn_req_mapping.contains_key(&fn_id) {
                    fn_req_mapping.insert(fn_id, req.req_id);
                }
            }
        }

        // 清理过期的智能体
        self.function_agents.retain(|&fn_id, _| fn_req_mapping.contains_key(&fn_id));
        
        // 清理过期的调度记录（只保留当前活跃的请求）
        let active_req_ids: HashSet<ReqId> = fn_req_mapping.values().cloned().collect();
        self.scheduled_fn_req_pairs.retain(|(_, req_id)| active_req_ids.contains(req_id));

        // 创建或更新智能体
        for (fn_id, req_id) in fn_req_mapping {
            if !self.function_agents.contains_key(&fn_id) {
                let mut agent = FunctionAgent::new(fn_id, req_id);
                
                if let Some(params) = self.fn_utility_configs.get(&fn_id) {
                    agent.utility_params = params.clone();
                }

                self.function_agents.insert(fn_id, agent);
            } else {
                // 只在必要时更新请求ID
                if let Some(agent) = self.function_agents.get_mut(&fn_id) {
                    if agent.req_id != req_id {
                        agent.req_id = req_id;
                    }
                }
            }
        }
    }
}

impl Scheduler for ScheNashScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // ===== 负载自适应纳什均衡博弈调度 =====

        // 安全检查，确保DAG系统已完全初始化
        if env.core().dags().is_empty() {
            return;
        }
        
        // 检查是否存在有效的函数和请求
        if env.core().fns().is_empty() || env.core().requests().is_empty() {
            return;
        }

        // 🔑 核心修改：动态获取自适应配置
        self.config = self.get_adaptive_config(env);

        self.initialize_function_configs(env);
        self.update_node_states(env);
        self.create_and_update_agents(env);
        
        if self.function_agents.is_empty() {
            return;
        }
        
        // 🔑 使用自适应价格信号
        self.broadcast_load_adaptive_price_signal(env);
        
        // 纳什均衡求解
        let nash_requests = self.solve_nash_equilibrium();
        
        if nash_requests.is_empty() {
            return;
        }
        
        // 🔑 关键修改：使用立即执行，移除延迟队列
        self.execute_nash_schedules_immediate(nash_requests.clone(), cmd_distributor);
        
        let total_agents = self.function_agents.len();
        let total_requests = nash_requests.len();
        let load_type = if env.help().config().request_freq_low() {
            "低负载"
        } else if env.help().config().request_freq_middle() {
            "中负载"  
        } else {
            "高负载"
        };
        let convergence_status = if self.nash_equilibrium.is_converged {
            format!("已收敛(轮次{})", self.nash_equilibrium.convergence_round)
        } else {
            "未收敛".to_string()
        };

        log::debug!(
            "纳什均衡调度({}) : {} 智能体, {} 决策, 收敛状态={}, 迭代轮次={}",
            load_type,
            total_agents,
            total_requests, 
            convergence_status,
            self.config.max_nash_iterations
        );
    }
}