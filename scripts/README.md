# OpenFaaS Motivation实验

使用真实的OpenFaaS仿真平台运行三个motivation实验，收集真实性能数据。

## 快速开始

```bash
cd scripts
python motivation_experiments.py
```

## 实验内容

### 实验1：外部性效应验证
- 测试资源竞争对性能的负面影响
- 5种调度器 × 3种请求频率 × 3种DAG类型
- 每组配置运行3次取平均值

### 实验2：偏好冲突验证
- 测试不同调度器对不同DAG类型的适应性
- 分析调度器-应用类型冲突矩阵
- 评估各调度器的满意度表现

### 实验3：现有方法局限验证
- 测试现有调度方法的可扩展性限制
- 分析不同规模下的性能退化
- 评估最大吞吐量限制

## 生成结果

### 数据文件
- `motivation_results/motivation_results_YYYYMMDD_HHMMSS.json` - 实验原始数据

### 图表文件
- `motivation_results/external_effects_results.png` - 外部性效应验证图表
- `motivation_results/preference_conflicts_results.png` - 偏好冲突验证图表
- `motivation_results/existing_limitations_results.png` - 现有方法局限验证图表

## 系统要求

- Python 3.8+
- Rust 1.70+ 和 Cargo
- Python包：numpy, pandas, matplotlib, requests

## 数据真实性

- 所有实验数据来自真实的serverless仿真系统
- 没有使用任何硬编码或模拟数据
- 符合学术研究的严格要求
- 实验结果完全可重现

## 运行时间

完整实验大约需要30-60分钟，取决于系统性能。脚本会：
1. 自动启动仿真服务器
2. 运行所有实验配置
3. 生成6个图表（每个实验2个图）
4. 保存原始数据和结果
5. 自动清理资源 