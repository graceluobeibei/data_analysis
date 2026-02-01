#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2026-01-24
@Description: Main script to generate dashboard JSON data (split into multiple files)
@Version: 3.0
"""

import sys
import os
import json
import math
import pandas as pd
import numpy as np

# Add src to sys.path to import main packages
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from main.config import get_input_filename, OUTPUT_PATH
from main.data_loader import load_data_pandas
from main.preprocess import preprocess_eleme_data
from main.analysis_modules.metrics import calculate_metrics
from main.analysis_modules.user import analyze_user
from main.analysis_modules.product import analyze_product
from main.analysis_modules.behavior import analyze_behavior
from main.analysis_modules.summary import generate_summary


def convert_to_json_serializable(o):
    """
    将numpy类型转换为JSON可序列化类型
    
    处理以下情况:
    1. pandas Index/Series -> list
    2. numpy 标量类型 -> Python 原生类型
    3. NaN/Infinity -> null (JSON标准不支持NaN)
    
    参数：
    ------
    o : any
        待转换的对象
    
    返回：
    ------
    any
        JSON可序列化的对象
    """
    # 处理 pandas 类型
    if isinstance(o, (pd.Index, pd.Series)):
        return o.tolist()
    
    # 处理 numpy 标量类型 (需要先检查 NaN)
    if hasattr(o, 'item'):
        val = o.item()
        # 检查是否为 NaN 或 Infinity
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    
    # 处理 Python float 的 NaN/Infinity
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    
    return o


def sanitize_for_json(obj):
    """
    递归遍历数据结构，将所有NaN/Infinity替换为None
    
    json.dump的default参数只对无法序列化的类型生效，
    而float('nan')是合法Python float，会被直接输出为JavaScript的NaN字面量。
    必须预处理数据才能确保JSON合规。
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.floating, np.integer)):
        val = obj.item()
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    elif isinstance(obj, (pd.Index, pd.Series)):
        return sanitize_for_json(obj.tolist())
    elif hasattr(obj, 'item'):
        return sanitize_for_json(obj.item())
    else:
        return obj


def save_json(data, filename):
    """
    保存数据为JSON文件
    
    参数：
    ------
    data : dict
        要保存的数据
    filename : str
        文件名
    """
    filepath = os.path.join(OUTPUT_PATH, filename)
    sanitized_data = sanitize_for_json(data)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(sanitized_data, f, ensure_ascii=False, indent=2, default=convert_to_json_serializable)
    print(f"  ✓ 生成: {filename}")


def main():
    print("🚀 开始生成仪表盘数据...")
    
    # 1. 加载数据
    try:
        df = load_data_pandas(get_input_filename())
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
        return

    # 2. 数据预处理
    df_clean = preprocess_eleme_data(df)
    
    # 3. 执行分析
    print("📊 运行分析模块...")
    
    # 创建输出目录
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    
    # 计算各模块数据
    metrics_res = calculate_metrics(df_clean)
    user_res = analyze_user(df_clean)
    prod_res = analyze_product(df_clean)
    beh_res = analyze_behavior(df_clean)
    sum_res = generate_summary(metrics_res, user_res)
    
    # 4. 保存为独立的JSON文件
    print("\n💾 保存分析结果...")
    save_json(metrics_res, 'metrics.json')
    save_json(user_res, 'user.json')
    save_json(prod_res, 'product.json')
    save_json(beh_res, 'behavior.json')
    save_json(sum_res, 'summary.json')
    
    # 5. 同时保存完整数据文件(向后兼容)
    final_data = {}
    final_data.update(metrics_res)
    final_data.update(user_res)
    final_data.update(prod_res)
    final_data.update(beh_res)
    final_data.update(sum_res)
    save_json(final_data, 'dashboard_data.json')
    
    print(f"\n✅ 所有数据文件生成成功!")
    print(f"📂 输出目录: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
