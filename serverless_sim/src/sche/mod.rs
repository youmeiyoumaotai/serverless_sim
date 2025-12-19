use bp_balance::BpBalanceScheduler;

use crate::{ config::Config, sim_run::Scheduler };

use self::{
    consistenthash::ConsistentHashScheduler,
    faasflow::FaasFlowScheduler,
    fnsche::FnScheScheduler,
    greedy::GreedyScheduler,
    pass::PassScheduler,
    pos::PosScheduler,
    random::RandomScheduler,
    hash::HashScheduler,
    rotate::RotateScheduler,
    ensure_scheduler::EnsureScheduler,
    load_least::LoadLeastScheduler,
    sche_nash::ScheNashScheduler,
    sche_orion::OrionScheduler,
    sche_jiagu::JiaguScheduler,
    sche_Hiku::HikuScheduler,
    sche_OCS::OCSScheduler,
    sche_FaaSRank::FaaSRankScheduler,
};

pub mod consistenthash;
pub mod faasflow;
pub mod fnsche;
pub mod greedy;
pub mod pass;
pub mod pos;
pub mod random;
pub mod bp_balance;
pub mod hash;
pub mod rotate;
pub mod ensure_scheduler;
pub mod load_least;
pub mod sche_nash;
pub mod sche_orion;
pub mod sche_jiagu;
pub mod sche_Hiku;
pub mod sche_OCS;
pub mod sche_FaaSRank;

pub fn prepare_spec_scheduler(config: &Config) -> Option<Box<dyn Scheduler + Send>> {
    let es = &config.mech;
    let (sche_name, sche_attr) = es.sche_conf();
    match &*sche_name {
        "faasflow" => {
            return Some(Box::new(FaasFlowScheduler::new()));
        }
        "pass" => {
            return Some(Box::new(PassScheduler::new()));
        }
        "pos" => {
            return Some(Box::new(PosScheduler::new(&sche_attr)));
        }
        "fnsche" => {
            return Some(Box::new(FnScheScheduler::new()));
        }
        "random" => {
            return Some(Box::new(RandomScheduler::new()));
        }
        "greedy" => {
            return Some(Box::new(GreedyScheduler::new()));
        }
        "bp_balance" => {
            return Some(Box::new(BpBalanceScheduler::new()));
        }
        "consistenthash" => {
            return Some(Box::new(ConsistentHashScheduler::new()));
        }
        "hash" => {
            return Some(Box::new(HashScheduler::new()));
        }
        "rotate" => {
            return Some(Box::new(RotateScheduler::new()));
        }
        "ensure_scheduler"=>{
            return Some(Box::new(EnsureScheduler::new()));
        }
        "load_least" => {
            return Some(Box::new(LoadLeastScheduler::new()));
        }
        "sche_nash" => {
            return Some(Box::new(ScheNashScheduler::new()));
        }
        "sche_orion" => {
            return Some(Box::new(OrionScheduler::new()));
        }
        "sche_jiagu" => {
            return Some(Box::new(JiaguScheduler::new()));
        }
        "sche_Hiku" => {
            return Some(Box::new(HikuScheduler::new()));
        }
        "sche_OCS" => {
            return Some(Box::new(OCSScheduler::new()));
        }
        "sche_FaaSRank" => {
            return Some(Box::new(FaaSRankScheduler::new()));
        }
        _ => {
            return None;
        }
    }
}
