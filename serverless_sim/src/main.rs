mod actions;
mod algos;
mod apis;
mod cache;
mod config;
mod env_gc;
mod fn_dag;
mod mechanism;
mod mechanism_conf;
mod mechanism_thread;
mod metric;
mod network;
mod node;
mod output;
mod request;
mod scale;
mod sche;
mod score;
mod sim_env;
mod sim_events;
mod sim_loop;
mod sim_run;
mod sim_timer;
mod state;
mod util;
mod with_env_sub;
mod rl_target;
mod dag_parsers;

use env_logger::{ Builder };
use log::LevelFilter;
use mechanism_conf::ModuleMechConf;
use std::io::Write;
use std::{ time::Duration };

#[macro_use]
extern crate lazy_static;

#[tokio::main]
async fn main() {
    let keyword: Vec<&'static str> = vec![];
        // vec!["::sche", "::mechanism ", "::scale"]; // no algo log
    Builder::new()
        .filter(None, LevelFilter::Info)
        .format(move |buf, record| {
            let message = format!("{} {}", record.module_path().unwrap_or("no_mod"), record.args());
            for k in &keyword {
                if message.contains(k) {
                    return Ok(());
                }
            }
            writeln!(buf, "{}: {}", record.level(), message)
        })
        .init();

    std::thread::sleep(Duration::from_secs(1));
    output::print_logo();
    // 启动垃圾回收（Garbage Collection, GC）机制
    env_gc::start_gc();
    ModuleMechConf::new().export_module_file();
    // parse_arg::parse_arg();
    network::start().await;
}

/* 
每1帧生成
节点数量修改为了30个
mix模式生成的应用为单函数、dag各5个
请求数量没变。
node的cpu资源从1000到200
*/

const REQUEST_GEN_FRAME_INTERVAL: usize = 1;//请求生成帧间隔
// const REQUEST_GEN_FRAME_INTERVAL: usize = 10;
//Nash算法更重视内存利用的个性化调度，降低CPU权重将削弱传统负载均衡算法的优势
const NODE_SCORE_CPU_WEIGHT: f32 = 0.2;

const NODE_SCORE_MEM_WEIGHT: f32 = 0.8;

// const NODE_CNT: usize = 10;
const NODE_CNT: usize = 500;//节点数量

const CONTAINER_BASIC_MEM: f32 = 300.0;//内存压力
//更严格的内存限制会让传统算法更容易遇到资源瓶颈，而Nash算法的价格机制能更好应对
const NODE_LEFT_MEM_THRESHOLD: f32 = 3500.0;//内存限制
