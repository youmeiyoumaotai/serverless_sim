#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试文件名解析逻辑
"""

import re
from pathlib import Path

def extract_algorithm_from_filename(filename: str) -> str:
    """
    从文件名中提取算法名称
    提取scd(XXX)中的XXX部分
    """
    pattern = r'scd\(([^)]+)\)'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return 'unknown'

def extract_load_type_from_filename(filename: str) -> str:
    """
    从文件名中提取负载类型
    """
    pattern = r'sd\.(rf\w+)\.'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    return 'unknown'

# 测试实际文件名
cache_dir = Path("cache")
if cache_dir.exists():
    json_files = list(cache_dir.glob("*.json"))
    print(f"找到 {len(json_files)} 个文件")
    print("\n文件名解析结果:")
    print("-" * 80)
    
    for json_file in json_files:
        filename = json_file.name
        load_type = extract_load_type_from_filename(filename)
        algorithm = extract_algorithm_from_filename(filename)
        
        print(f"文件: {filename[:60]}...")
        print(f"  负载类型: {load_type}")
        print(f"  算法名称: {algorithm}")
        print()
else:
    print("cache目录不存在")