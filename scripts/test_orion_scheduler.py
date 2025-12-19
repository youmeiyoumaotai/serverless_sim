#!/usr/bin/env python3
"""
Orion调度器测试脚本
测试基于DAG性能模型和预预热优化的Orion调度算法
"""

import subprocess
import sys
import os
import json
import time
from pathlib import Path

def run_orion_test():
    """运行Orion调度器测试"""
    
    print("🚀 开始Orion调度器测试...")
    
    # 测试配置
    test_configs = [
        {
            "name": "Orion低负载测试",
            "request_freq": "low",
            "dag_type": "mix",
            "scheduler": "sche_orion"
        },
        {
            "name": "Orion中负载测试", 
            "request_freq": "middle",
            "dag_type": "mix",
            "scheduler": "sche_orion"
        },
        {
            "name": "Orion高负载测试",
            "request_freq": "high", 
            "dag_type": "mix",
            "scheduler": "sche_orion"
        }
    ]
    
    results = []
    
    for config in test_configs:
        print(f"\n📊 运行测试: {config['name']}")
        
        # 创建临时配置文件
        temp_config = f"""
run_time: 3

params:
  request_freq:
  - {config['request_freq']}:
  dag_type:
  - {config['dag_type']}:
  no_mech_latency:
  - true:

mech_scale_sche:
  scale_sche_separated:
    scale_num:
    - hpa:
    scale_down_exec:
    - default:
    scale_up_exec:
    - least_task:
    sche:
    - {config['scheduler']}:
    filter:
    - [{{'careful_down':''}}]

mech_other:
  instance_cache_policy:
  - no_evict:
"""
        
        # 写入临时配置文件
        with open("temp_orion_test.yml", "w") as f:
            f.write(temp_config)
        
        try:
            # 运行测试
            start_time = time.time()
            result = subprocess.run([
                "python", "scripts/batch_run.py",
                "--config", "temp_orion_test.yml"
            ], capture_output=True, text=True, timeout=60)
            end_time = time.time()
            
            # 解析结果
            test_result = {
                "name": config['name'],
                "success": result.returncode == 0,
                "duration": end_time - start_time,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
            if test_result["success"]:
                print(f"✅ {config['name']} 测试成功 (耗时: {test_result['duration']:.2f}s)")
            else:
                print(f"❌ {config['name']} 测试失败")
                print(f"错误信息: {result.stderr}")
            
            results.append(test_result)
            
        except subprocess.TimeoutExpired:
            print(f"⏰ {config['name']} 测试超时")
            results.append({
                "name": config['name'],
                "success": False,
                "duration": 60,
                "stdout": "",
                "stderr": "测试超时"
            })
        except Exception as e:
            print(f"💥 {config['name']} 测试异常: {e}")
            results.append({
                "name": config['name'],
                "success": False,
                "duration": 0,
                "stdout": "",
                "stderr": str(e)
            })
        
        # 清理临时文件
        if os.path.exists("temp_orion_test.yml"):
            os.remove("temp_orion_test.yml")
    
    # 生成测试报告
    print("\n📋 Orion调度器测试报告")
    print("=" * 50)
    
    successful_tests = sum(1 for r in results if r["success"])
    total_tests = len(results)
    
    print(f"总测试数: {total_tests}")
    print(f"成功测试: {successful_tests}")
    print(f"失败测试: {total_tests - successful_tests}")
    print(f"成功率: {successful_tests/total_tests*100:.1f}%")
    
    print("\n详细结果:")
    for result in results:
        status = "✅" if result["success"] else "❌"
        duration = f"{result['duration']:.2f}s" if result['duration'] > 0 else "N/A"
        print(f"{status} {result['name']} - {duration}")
    
    # 保存详细结果到文件
    with open("orion_test_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n详细结果已保存到: orion_test_results.json")
    
    return successful_tests == total_tests

def compare_with_baseline():
    """与基准算法比较性能"""
    
    print("\n🔍 与基准算法性能比较...")
    
    baseline_config = """
run_time: 3

params:
  request_freq:
  - middle:
  dag_type:
  - mix:
  no_mech_latency:
  - true:

mech_scale_sche:
  scale_sche_separated:
    scale_num:
    - hpa:
    scale_down_exec:
    - default:
    scale_up_exec:
    - least_task:
    sche:
    - greedy:
    filter:
    - [{'careful_down':''}]

mech_other:
  instance_cache_policy:
  - no_evict:
"""
    
    orion_config = """
run_time: 3

params:
  request_freq:
  - middle:
  dag_type:
  - mix:
  no_mech_latency:
  - true:

mech_scale_sche:
  scale_sche_separated:
    scale_num:
    - hpa:
    scale_down_exec:
    - default:
    scale_up_exec:
    - least_task:
    sche:
    - sche_orion:
    filter:
    - [{'careful_down':''}]

mech_other:
  instance_cache_policy:
  - no_evict:
"""
    
    # 运行基准测试
    with open("temp_baseline.yml", "w") as f:
        f.write(baseline_config)
    
    with open("temp_orion.yml", "w") as f:
        f.write(orion_config)
    
    try:
        print("运行Greedy基准测试...")
        baseline_result = subprocess.run([
            "python", "scripts/batch_run.py",
            "--config", "temp_baseline.yml"
        ], capture_output=True, text=True, timeout=60)
        
        print("运行Orion测试...")
        orion_result = subprocess.run([
            "python", "scripts/batch_run.py", 
            "--config", "temp_orion.yml"
        ], capture_output=True, text=True, timeout=60)
        
        print("\n性能比较结果:")
        print(f"Greedy基准测试: {'成功' if baseline_result.returncode == 0 else '失败'}")
        print(f"Orion测试: {'成功' if orion_result.returncode == 0 else '失败'}")
        
        if baseline_result.returncode == 0 and orion_result.returncode == 0:
            print("✅ 两个算法都成功运行，可以进行性能比较")
        else:
            print("❌ 部分测试失败，无法进行完整比较")
            
    except Exception as e:
        print(f"💥 性能比较测试异常: {e}")
    finally:
        # 清理临时文件
        for temp_file in ["temp_baseline.yml", "temp_orion.yml"]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

if __name__ == "__main__":
    print("🎯 Orion调度器测试套件")
    print("=" * 50)
    
    # 检查必要文件
    required_files = [
        "scripts/batch_run.py",
        "serverless_sim/src/sche/sche_orion.rs"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ 缺少必要文件:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        sys.exit(1)
    
    # 运行测试
    success = run_orion_test()
    
    # 运行性能比较
    compare_with_baseline()
    
    if success:
        print("\n🎉 所有Orion调度器测试通过!")
        sys.exit(0)
    else:
        print("\n💥 部分Orion调度器测试失败!")
        sys.exit(1) 