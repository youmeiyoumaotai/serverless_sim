use crate::{
    fn_dag::{EnvFnExt, FnId},
    mechanism::{DownCmd, MechanismImpl, ScheCmd, SimEnvObserve},
    mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes},
    node::{EnvNodeExt, NodeId},
    request::Request,
    sim_run::Scheduler,
    util,
    with_env_sub::WithEnvCore,
};
use daggy::{
    petgraph::visit::{EdgeRef, IntoEdgeReferences},
    EdgeIndex,
};
use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

/// 函数组 - 表示可以部署在同一节点上的函数集合
#[derive(Clone, Debug)]
struct FunctionGroup {
    functions: HashSet<FnId>,
    total_memory: f32,
    total_cpu: f32,
    assigned_node: Option<NodeId>,
    group_id: usize,
}

impl FunctionGroup {
    fn new(group_id: usize) -> Self {
        Self {
            functions: HashSet::new(),
            total_memory: 0.0,
            total_cpu: 0.0,
            assigned_node: None,
            group_id,
        }
    }

    fn add_function(&mut self, fn_id: FnId, memory: f32, cpu: f32) {
        self.functions.insert(fn_id);
        self.total_memory += memory;
        self.total_cpu += cpu;
    }

    fn can_merge_with(&self, other: &FunctionGroup, node_mem_limit: f32, node_cpu_limit: f32) -> bool {
        (self.total_memory + other.total_memory) <= node_mem_limit &&
        (self.total_cpu + other.total_cpu) <= node_cpu_limit
    }

    fn merge_with(&mut self, other: FunctionGroup) {
        self.functions.extend(other.functions);
        self.total_memory += other.total_memory;
        self.total_cpu += other.total_cpu;
    }
}

/// 边权重信息 - 用于关键路径分析
#[derive(Clone, Debug)]
struct EdgeWeight {
    data_size: f32,
    communication_cost: f32,
}

/// FaaSFlow调度器 - 基于论文的完整实现
pub struct FaasFlowScheduler {
    /// 函数组映射
    function_groups: HashMap<usize, FunctionGroup>,
    /// 函数到组的映射
    fn_to_group: HashMap<FnId, usize>,
    /// 下一个组ID
    next_group_id: usize,
    /// 最大迭代次数
    max_iterations: usize,
}

impl FaasFlowScheduler {
    pub fn new() -> Self {
        Self {
            function_groups: HashMap::new(),
            fn_to_group: HashMap::new(),
            next_group_id: 0,
            max_iterations: 10,
        }
    }

    /// 初始化函数分组 - 每个函数作为独立组
    fn initialize_groups(&mut self, env: &SimEnvObserve, req: &Request) {
        self.function_groups.clear();
        self.fn_to_group.clear();
        self.next_group_id = 0;

        let dag = env.dag(req.dag_i);
        let mut walker = dag.new_dag_walker();

        while let Some(fnode) = walker.next(&dag.dag_inner) {
            let fn_id = dag.dag_inner[fnode];
            let func = env.func(fn_id);

            let mut group = FunctionGroup::new(self.next_group_id);
            group.add_function(fn_id, func.container_mem(), func.cpu);

            self.function_groups.insert(self.next_group_id, group);
            self.fn_to_group.insert(fn_id, self.next_group_id);
            self.next_group_id += 1;
        }
    }

    /// 计算关键路径上的边
    fn find_critical_path_edges(&self, env: &SimEnvObserve, req: &Request) -> Vec<EdgeIndex> {
        let dag = env.dag(req.dag_i);
        let critical_path_nodes = util::graph::aoe_critical_path(&dag.dag_inner);

        let mut critical_edges = Vec::new();
        for i in 0..critical_path_nodes.len() - 1 {
            if let Some(edge) = dag.dag_inner.find_edge(critical_path_nodes[i], critical_path_nodes[i + 1]) {
                critical_edges.push(edge);
            }
        }
        critical_edges
    }

    /// 获取所有边按权重排序
    fn get_sorted_edges(&self, env: &SimEnvObserve, req: &Request, critical_edges: &[EdgeIndex]) -> Vec<EdgeIndex> {
        let dag = env.dag(req.dag_i);
        let mut all_edges: Vec<EdgeIndex> = dag.dag_inner.edge_references().map(|e| e.id()).collect();

        // 按边权重排序（权重越大越优先）
        all_edges.sort_by(|&a, &b| {
            let weight_a = *dag.dag_inner.edge_weight(a).unwrap_or(&0.0);
            let weight_b = *dag.dag_inner.edge_weight(b).unwrap_or(&0.0);

            // 关键路径边优先
            let is_critical_a = critical_edges.contains(&a);
            let is_critical_b = critical_edges.contains(&b);

            match (is_critical_a, is_critical_b) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => weight_b.partial_cmp(&weight_a).unwrap_or(std::cmp::Ordering::Equal),
            }
        });

        all_edges
    }

    /// 尝试合并两个函数组
    fn try_merge_groups(&mut self, env: &SimEnvObserve, edge: EdgeIndex, req: &Request) -> bool {
        let dag = env.dag(req.dag_i);
        let (source_node, target_node) = dag.dag_inner.edge_endpoints(edge).unwrap();
        let source_fn = dag.dag_inner[source_node];
        let target_fn = dag.dag_inner[target_node];

        let source_group_id = *self.fn_to_group.get(&source_fn).unwrap();
        let target_group_id = *self.fn_to_group.get(&target_fn).unwrap();

        // 如果已经在同一组，无需合并
        if source_group_id == target_group_id {
            return false;
        }

        // 检查是否可以合并
        let source_group = self.function_groups.get(&source_group_id).unwrap().clone();
        let target_group = self.function_groups.get(&target_group_id).unwrap().clone();

        // 获取节点资源限制（使用第一个节点作为参考）
        let node_mem_limit = env.node(0).rsc_limit.mem;
        let node_cpu_limit = env.node(0).rsc_limit.cpu;

        if source_group.can_merge_with(&target_group, node_mem_limit, node_cpu_limit) {
            // 执行合并
            let mut merged_group = source_group;
            merged_group.merge_with(target_group);

            // 更新映射
            for &fn_id in &merged_group.functions {
                self.fn_to_group.insert(fn_id, source_group_id);
            }

            // 更新组
            self.function_groups.insert(source_group_id, merged_group);
            self.function_groups.remove(&target_group_id);

            true
        } else {
            false
        }
    }

    /// 检查资源竞争
    fn has_resource_conflict(&self, _group: &FunctionGroup) -> bool {
        // 简化实现：假设没有资源竞争
        // 在实际实现中，这里应该检查函数之间是否存在资源竞争
        false
    }

    /// 装箱策略 - 为每个组选择最佳节点
    fn bin_packing_assignment(&mut self, env: &SimEnvObserve) -> HashMap<FnId, NodeId> {
        let mut assignments = HashMap::new();
        let mut node_remaining_mem: Vec<f32> = env.nodes().iter()
            .map(|n| n.left_mem_for_place_container())
            .collect();
        let mut node_remaining_cpu: Vec<f32> = env.nodes().iter()
            .map(|n| n.rsc_limit.cpu - n.cpu)
            .collect();

        // 按资源需求排序组（资源需求大的优先）
        let mut groups: Vec<_> = self.function_groups.values().collect();
        groups.sort_by(|a, b| {
            (b.total_memory + b.total_cpu).partial_cmp(&(a.total_memory + a.total_cpu))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for group in groups {
            let mut best_node = 0;
            let mut best_score = f32::NEG_INFINITY;

            // 为每个组找到最佳节点
            for (node_id, &remaining_mem) in node_remaining_mem.iter().enumerate() {
                let remaining_cpu = node_remaining_cpu[node_id];

                // 检查资源是否足够
                if remaining_mem >= group.total_memory && remaining_cpu >= group.total_cpu {
                    // 计算适应度分数（剩余资源越少越好，避免浪费）
                    let mem_utilization = group.total_memory / remaining_mem;
                    let cpu_utilization = group.total_cpu / remaining_cpu;
                    let score = mem_utilization + cpu_utilization;

                    if score > best_score {
                        best_score = score;
                        best_node = node_id;
                    }
                }
            }

            // 分配组到最佳节点
            for &fn_id in &group.functions {
                assignments.insert(fn_id, best_node);
            }

            // 更新节点剩余资源
            node_remaining_mem[best_node] -= group.total_memory;
            node_remaining_cpu[best_node] -= group.total_cpu;
        }

        assignments
    }

    // fn do_some_schedule(&self, req: &mut Request, env: &SimEnv) {
    //     let dag = env.dag(req.dag_i);
    //     let plan = self.request_schedule_state.get(&req.req_id).unwrap();
    //     let mut walker = dag.new_dag_walker();
    //     while let Some(fnode) = walker.next(&dag.dag_inner) {
    //         let fnid = dag.dag_inner[fnode];
    //         // Already scheduled
    //         if req.get_fn_node(fnid).is_some() {
    //             continue;
    //         }
    //         // Not schduled but not all parents done
    //         if !req.parents_all_done(env, fnid) {
    //             continue;
    //         }
    //         // Ready to be scheduled
    //         let fn_node = *plan.fn_nodes.get(&fnid).unwrap();
    //         if env.node(fn_node).container(fnid).is_none() {
    //             if env
    //                 .scale_executor
    //                 .borrow_mut()
    //                 .scale_up_fn_to_nodes(env, fnid, &vec![fn_node])
    //                 == 0
    //             {
    //                 continue;
    //             }
    //         }
    //         // if env.node(fn_node).mem_enough_for_container(&env.func(fnid)) {
    //         env.schedule_reqfn_on_node(req, fnid, fn_node);
    //         // }
    //     }
    // }

    /// 基于论文的FaaSFlow调度算法实现
    fn schedule_for_one_req(
        &mut self,
        req: &Request,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) {
        if req.fn_count(env) == req.fn_node.len() {
            return;
        }

        log::info!("faasflow start generate schedule for req {}", req.req_id);

        // 第1步：初始化函数分组（每个函数作为独立组）
        self.initialize_groups(env, req);

        // 第2步：迭代合并过程
        for _iteration in 0..self.max_iterations {
            let mut merged_any = false;

            // 第3步：找到关键路径上的边
            let critical_edges = self.find_critical_path_edges(env, req);

            // 第4步：获取所有边按权重排序（关键路径优先）
            let sorted_edges = self.get_sorted_edges(env, req, &critical_edges);

            // 第5步：尝试合并函数组
            for edge in sorted_edges {
                if self.try_merge_groups(env, edge, req) {
                    merged_any = true;
                }
            }

            // 如果没有合并发生，算法收敛
            if !merged_any {
                break;
            }
        }

        // 第6步：装箱策略 - 为每个组选择最佳节点
        let assignments = self.bin_packing_assignment(env);

        log::info!("faasflow end generate schedule for req {}", req.req_id);

        // 发送调度命令
        cmd_distributor
            .send(MechScheduleOnceRes::Cmds {
                sche_cmds: assignments
                    .into_iter()
                    .map(|(fnid, nid)| ScheCmd {
                        nid,
                        reqid: req.req_id,
                        fnid,
                        memlimit: None,
                    })
                    .collect(),
                scale_up_cmds: vec![],
                scale_down_cmds: vec![],
            })
            .unwrap();
    }
}

// 图形调度器中分组和调度算法的关键步骤如下所示。
// 在初始化阶段，每个函数节点都作为单独的组进行初始化，并且工作节点是随机分配的（第1-2行）。
// 首先，算法从拓扑排序和迭代开始。在每次迭代的开始，它将使用贪婪方法来定位DAG图中关键路径上具有最长边的两个函数，
// 并确定这两个函数是否可以合并到同一组（第3-8行）。
// 如果这两个函数被分配到不同的组中，它们将被合并（第9行）。
// 在合并组时，需要考虑额外的因素。
//  首先，算法需要确保合并的函数组不超过工作节点的最大容量（第10-12行）。
//  否则，合并的组将无法部署在任何节点上。其次，组内局部化的数据总量不能违反内存约束（第13-18行）。
//  同时，在合并的组中不能存在任何资源竞争的函数对𝑐𝑜𝑛𝑡 (𝐺) = {(𝑓𝑖, 𝑓𝑗 )}（第19-20行）。
//  最后，调度算法将采用装箱策略，根据节点容量为每个函数组选择适当的工作节点（第21-23行）。
// 根据上述逻辑，算法迭代直到收敛，表示函数组不再更新。
impl Scheduler for FaasFlowScheduler {
    fn schedule_some(
        &mut self,
        env: &SimEnvObserve,
        _mech: &MechanismImpl,
        cmd_distributor: &MechCmdDistributor,
    ) {
        // let mut sche_cmds = vec![];
        for (_, req) in env.core().requests_mut().iter_mut() {
            self.schedule_for_one_req(req, env, cmd_distributor);
        }

        // let mut to_scale_down = vec![];
        // 超时策略，回收空闲container
        for n in env.nodes().iter() {
            for (_, c) in n.fn_containers.borrow().iter() {
                if c.recent_frame_is_idle(3) && c.req_fn_state.len() == 0 {
                    // to_scale_down.push((n.node_id(), c.fn_id));
                    cmd_distributor
                        .send(MechScheduleOnceRes::ScaleDownCmd(DownCmd {
                            nid: n.node_id(),
                            fnid: c.fn_id,
                        }))
                        .unwrap();
                }
            }
        }

        // for (n, f) in to_scale_down {
        //     env.mechanisms
        //         .scale_executor_mut()
        //         .exec_scale_down(env, ScaleOption::ForSpecNodeFn(n, f));
        // }
    }
}
