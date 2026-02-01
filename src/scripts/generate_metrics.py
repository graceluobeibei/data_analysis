#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2026-01-25
@Description: Generate metrics analysis data independently
@Version: 1.0
"""

import sys
import os
import json
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from main.config import get_input_filename, OUTPUT_PATH
from main.data_loader import load_data_pandas
from main.preprocess import preprocess_eleme_data
from main.analysis_modules.metrics import calculate_metrics


def convert_to_json_serializable(o):
    if isinstance(o, (pd.Index, pd.Series)):
        return o.tolist()
    if hasattr(o, 'item'):
        return o.item()
    return o


def main():
    print("📊 生成核心指标数据...")
    
    df = load_data_pandas(get_input_filename())
    df_clean = preprocess_eleme_data(df)
    
    metrics_res = calculate_metrics(df_clean)
    
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    
    output_file = os.path.join(OUTPUT_PATH, 'metrics.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metrics_res, f, ensure_ascii=False, indent=2, default=convert_to_json_serializable)
    
    print(f"✅ 指标数据已生成: {output_file}")


if __name__ == "__main__":
    main()
