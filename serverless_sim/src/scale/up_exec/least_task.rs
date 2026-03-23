use super::ScaleUpExec;
use crate::cache::gvgc::global_fn_score;
use crate::mechanism_thread::{MechCmdDistributor, MechScheduleOnceRes};
use crate::node::EnvNodeExt;
use crate::with_env_sub::WithEnvHelp;
use crate::{
    fn_dag::FnId,
    mechanism::{SimEnvObserve, UpCmd},
};

pub struct LeastTaskScaleUpExec;

impl LeastTaskScaleUpExec {
    pub fn new() -> Self {
        LeastTaskScaleUpExec {}
    }
}

impl ScaleUpExec for LeastTaskScaleUpExec {
    fn exec_scale_up(
        &self,
        target_cnt: usize,
        fnid: FnId,
        env: &SimEnvObserve,
        cmd_distributor: &MechCmdDistributor,
    ) -> Vec<UpCmd> {
        let mech_metric = || env.help().mech_metric_mut();
        let mut up_cmds = vec![];

        let mut nodes_no_container = env
            .nodes()
            .iter()
            .filter(|n| n.container(fnid).is_none())
            .map(|n| n.node_id())
            .collect::<Vec<_>>();

        let nodes_with_container_cnt = env.nodes().len() - nodes_no_container.len();

        if nodes_with_container_cnt < target_cnt && !nodes_no_container.is_empty() {
            let to_scale_up_cnt = std::cmp::min(
                target_cnt - nodes_with_container_cnt,
                nodes_no_container.len(),
            );

            let cache_policy = env.help().config().mech.instance_cache_policy_conf().0;
            if cache_policy == "gvgc" {
                // GVGC: choose candidate node with lowest hot-pressure score.
                nodes_no_container.sort_by(|&a, &b| {
                    let node_hot_pressure = |nid| {
                        let node = env.node(nid);
                        let score = node
                            .fn_containers
                            .borrow()
                            .keys()
                            .map(|fid| global_fn_score(*fid))
                            .sum::<f32>();
                        score
                    };
                    let ap = node_hot_pressure(a);
                    let bp = node_hot_pressure(b);
                    ap.partial_cmp(&bp).unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                // Keep original behavior: prefer nodes with fewer tasks.
                nodes_no_container.sort_by(|&a, &b| {
                    let acnt = mech_metric().node_task_new_cnt(a);
                    let bcnt = mech_metric().node_task_new_cnt(b);
                    acnt.partial_cmp(&bcnt).unwrap()
                });
            }

            nodes_no_container.reverse();
            for _ in 0..to_scale_up_cnt {
                let node_2_load_contaienr = nodes_no_container.pop().unwrap();
                let _ = cmd_distributor.send(MechScheduleOnceRes::ScaleUpCmd(UpCmd {
                    nid: node_2_load_contaienr,
                    fnid,
                }));
                up_cmds.push(UpCmd {
                    nid: node_2_load_contaienr,
                    fnid,
                })
            }
        }

        up_cmds
    }
}
