#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重新生成图表的简单脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from openfaas_simulator import OpenFaaSMotivationExperiments

def main():
    print("开始重新生成图表...")
    
    # 创建实验对象
    experiments = OpenFaaSMotivationExperiments()
    
    # 重新生成图表
    print("重新生成问题1图表...")
    experiments._plot_resource_competition_analysis(experiments._load_existing_data())
    
    print("重新生成问题2图表...")
    experiments._plot_preference_conflicts_analysis(experiments._load_existing_data())
    
    print("图表重新生成完成！")

if __name__ == "__main__":
    main() 