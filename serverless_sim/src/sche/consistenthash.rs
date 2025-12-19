use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{MechanismImpl, ScheCmd, SimEnvObserve, UpCmd},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::Request,
    sim_run::{schedule_helper, Scheduler},
    with_env_sub::WithEnvCore,
};

use std::{
    collections::{BTreeMap, HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

/// 虚拟节点结构
#[derive(Clone, Debug)]
struct VirtualNode {
    node_id: NodeId,
    virtual_id: usize,
    hash_value: u64,
}

/// 一致性哈希环
#[derive(Clone, Debug)]
struct HashRing {
    /// 虚拟节点映射 (hash_value -> VirtualNode)
    virtual_nodes: BTreeMap<u64, VirtualNode>,
    /// 每个物理节点的虚拟节点数量
    virtual_nodes_per_node: usize,
    /// 节点负载统计
    node_loads: HashMap<NodeId, f32>,
}

impl HashRing {
    fn new(virtual_nodes_per_node: usize) -> Self {
        Self {
            virtual_nodes: BTreeMap::new(),
            virtual_nodes_per_node,
            node_loads: HashMap::new(),
        }
    }

    /// 添加物理节点到哈希环
    fn add_node(&mut self, node_id: NodeId) {
        for i in 0..self.virtual_nodes_per_node {
            let mut hasher = DefaultHasher::new();
            format!("node-{}-virtual-{}", node_id, i).hash(&mut hasher);
            let hash_value = hasher.finish();

            let virtual_node = VirtualNode {
                node_id,
                virtual_id: i,
                hash_value,
            };

            self.virtual_nodes.insert(hash_value, virtual_node);
        }
        self.node_loads.insert(node_id, 0.0);
    }

    /// 移除物理节点从哈希环
    fn remove_node(&mut self, node_id: NodeId) {
        self.virtual_nodes.retain(|_, vnode| vnode.node_id != node_id);
        self.node_loads.remove(&node_id);
    }

    /// 根据键找到对应的节点
    fn get_node(&self, key: &str) -> Option<NodeId> {
        if self.virtual_nodes.is_empty() {
            return None;
        }

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();

        // 找到第一个大于等于hash_value的虚拟节点
        if let Some((_, vnode)) = self.virtual_nodes.range(hash_value..).next() {
            Some(vnode.node_id)
        } else {
            // 如果没找到，返回环上的第一个节点（环形特性）
            self.virtual_nodes.values().next().map(|vnode| vnode.node_id)
        }
    }

    /// 获取负载最小的节点（用于负载均衡）
    fn get_least_loaded_node(&self, candidates: &[NodeId]) -> Option<NodeId> {
        candidates.iter()
            .min_by(|&&a, &&b| {
                let load_a = self.node_loads.get(&a).unwrap_or(&0.0);
                let load_b = self.node_loads.get(&b).unwrap_or(&0.0);
                load_a.partial_cmp(load_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
    }

    /// 更新节点负载
    fn update_node_load(&mut self, node_id: NodeId, load: f32) {
        self.node_loads.insert(node_id, load);
    }
}

/// 一致性哈希调度器
pub struct ConsistentHashScheduler {
    /// 哈希环
    hash_ring: HashRing,
    /// 负载均衡阈值
    load_balance_threshold: f32,
    /// 副本因子（为容错选择多个候选节点）
    replication_factor: usize,
}

impl ConsistentHashScheduler {
    pub fn new() -> Self {
        Self {
            hash_ring: HashRing::new(150), // 每个物理节点150个虚拟节点
            load_balance_threshold: 0.8,
            replication_factor: 3,
        }
    }

    /// 初始化哈希环
    fn initialize_hash_ring(&mut self, env: &SimEnvObserve) {
        // 清空现有环
        self.hash_ring = HashRing::new(150);

        // 添加所有节点到环中
        for node in env.nodes().iter() {
            self.hash_ring.add_node(node.node_id());
        }
    }

    /// 更新节点负载信息
    fn update_node_loads(&mut self, env: &SimEnvObserve) {
        for node in env.nodes().iter() {
            let load = node.unready_mem() / node.rsc_limit.mem;
            self.hash_ring.update_node_load(node.node_id(), load);
        }
    }

    /// 获取函数的候选节点列表（用于容错）
    fn get_candidate_nodes(&self, fn_key: &str) -> Vec<NodeId> {
        let mut candidates = Vec::new();
        let mut seen_nodes = std::collections::HashSet::new();

        // 从哈希环中获取多个候选节点
        let mut hasher = DefaultHasher::new();
        fn_key.hash(&mut hasher);
        let mut hash_value = hasher.finish();

        for _ in 0..self.replication_factor {
            if let Some((_, vnode)) = self.hash_ring.virtual_nodes.range(hash_value..).next() {
                if !seen_nodes.contains(&vnode.node_id) {
                    candidates.push(vnode.node_id);
                    seen_nodes.insert(vnode.node_id);
                }
                hash_value = vnode.hash_value + 1;
            } else if let Some((_, vnode)) = self.hash_ring.virtual_nodes.iter().next() {
                if !seen_nodes.contains(&vnode.node_id) {
                    candidates.push(vnode.node_id);
                    seen_nodes.insert(vnode.node_id);
                }
                hash_value = vnode.hash_value + 1;
            }
        }

        candidates
    }

    /// 基于一致性哈希的函数调度
    fn schedule_one_req_fns(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        req: &mut Request,
        cmd_distributor: &MechCmdDistributor,
    ) {
        let fns = schedule_helper::collect_task_to_sche(
            req,
            env,
            schedule_helper::CollectTaskConfig::All,
        );

        for fnid in fns {
            let target_cnt = mech.scale_num(fnid).max(1);

            // 生成函数的唯一键
            let fn_key = format!("fn-{}-req-{}", fnid, req.req_id);

            // 获取候选节点列表
            let candidates = self.get_candidate_nodes(&fn_key);

            // 选择最佳节点（负载均衡）
            let selected_node = if candidates.is_empty() {
                // 如果没有候选节点，使用简单哈希
                let mut hasher = DefaultHasher::new();
                fnid.hash(&mut hasher);
                hasher.finish() as usize % env.node_cnt()
            } else {
                // 从候选节点中选择负载最小的
                let available_candidates: Vec<NodeId> = candidates.iter()
                    .filter(|&&node_id| {
                        let node = env.node(node_id);
                        let mem_usage = node.unready_mem() / node.rsc_limit.mem;
                        mem_usage < self.load_balance_threshold
                    })
                    .copied()
                    .collect();

                if available_candidates.is_empty() {
                    // 如果所有候选节点都过载，选择负载最小的
                    self.hash_ring.get_least_loaded_node(&candidates).unwrap_or(0)
                } else {
                    // 从可用候选节点中选择负载最小的
                    self.hash_ring.get_least_loaded_node(&available_candidates).unwrap_or(available_candidates[0])
                }
            };

            // 发送调度命令
            cmd_distributor
                .send(MechScheduleOnceRes::ScheCmd(ScheCmd {
                    nid: selected_node,
                    reqid: req.req_id,
                    fnid,
                    memlimit: None,
                }))
                .unwrap();

            // 处理扩容
            for i in 0..target_cnt {
                let node_id = if i == 0 {
                    selected_node
                } else {
                    // 为额外的副本选择其他节点
                    (selected_node + i) % env.node_cnt()
                };

                let node = env.node(node_id);
                if node.container(fnid).is_none() {
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScaleUpCmd(UpCmd {
                            nid: node_id,
                            fnid,
                        }))
                        .unwrap();
                }
            }
        }
    }
}

impl Scheduler for ConsistentHashScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // 初始化哈希环（如果需要）
        if self.hash_ring.virtual_nodes.is_empty() {
            self.initialize_hash_ring(env);
        }

        // 更新节点负载信息
        self.update_node_loads(env);

        // 处理缩容
        for func in env.core().fns().iter() {
            let target = mech.scale_num(func.fn_id);
            let cur = env.fn_container_cnt(func.fn_id);
            if target < cur {
                mech.scale_down_exec().exec_scale_down(
                    env,
                    func.fn_id,
                    cur - target,
                    cmd_distributor,
                );
            }
        }

        // 处理请求调度
        for (_req_id, req) in env.core().requests_mut().iter_mut() {
            self.schedule_one_req_fns(env, mech, req, cmd_distributor);
        }
    }
}
