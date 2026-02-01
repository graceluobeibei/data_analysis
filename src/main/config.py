#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: Configuration file for data paths, column names, and mappings.
@Version: 2.0
"""

import os
import sys

# Data Paths
DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/")) + "/"
PROCESSED_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed/")) + "/"
OUTPUT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../output/")) + "/"

# Input File
# INPUT_FILENAME = "D1_0_top_10k.csv"
# INPUT_FILENAME 可通过命令行参数或环境变量覆盖（命令行优先）
# 使用示例：
#   python src/scripts/generate_metrics.py --input-file=D1_0_top_10k.csv
# 或在 notebook 中：
#   import os; os.environ['INPUT_FILENAME'] = 'D1_0_top_10k.csv'    

def get_input_filename() -> str:
    """
    获取输入文件名，优先使用命令行参数(--input-file= 或 --input-filename=)，
    其次使用环境变量 INPUT_FILENAME，默认 'D1_0_top_10k.csv'
    
    注意：此函数每次调用都会重新读取环境变量，适用于 Jupyter Notebook 等需要动态修改配置的场景
    """
    for arg in sys.argv:
        if arg.startswith('--input-file=') or arg.startswith('--input-filename='):
            return arg.split('=', 1)[1]
    return os.environ.get('INPUT_FILENAME', 'D1_0_top_3.csv')

# 向后兼容：模块加载时的默认值（注意：在 Jupyter 中设置环境变量后此值不会自动更新，请使用 get_input_filename()）
INPUT_FILENAME = get_input_filename()

# Output File
OUTPUT_JSON_FILENAME = "dashboard_data.json"

# Dask Settings
PARTITIONS = 8

# Column Names (Based on requirement doc and data sample)
COLUMN_NAMES = [
    "label",
    "user_id",
    "gender",
    "visit_city",
    "avg_price",
    "is_supervip",
    "ctr_30",
    "ord_30",
    "total_amt_30",
    "shop_id",
    "item_id",
    "city_id",
    "district_id",
    "shop_aoi_id",
    "shop_geohash_6",
    "shop_geohash_12",
    "brand_id",
    "category_1_id",
    "merge_standard_food_id",
    "rank_7",
    "rank_30",
    "rank_90",
    "shop_id_list",
    "item_id_list",
    "category_1_id_list",
    "merge_standard_food_id_list",
    "brand_id_list",
    "price_list",
    "shop_aoi_id_list",
    "shop_geohash6_list",
    "timediff_list",
    "hours_list",
    "time_type_list",
    "weekdays_list",
    "times",
    "hours",
    "time_type",
    "weekdays",
    "geohash12"
]

# Mappings (Mock data for visualization purposes as real mappings are not provided)
CITY_MAPPING = {
    2: "北京",
    7: "上海",
    3: "深圳",
    15: "广州",
    6: "杭州",
    66: "成都",
    35: "武汉",
    2081: "西安",
    5322: "重庆"
}

CATEGORY_MAPPING = {
    1001: "快餐简餐",
    1066: "地方菜系",
    1059: "甜品饮品",
    1023: "火锅烧烤",
    1015: "异国料理",
    1048: "小吃夜宵"
}

# Default mappings for unknown IDs
def get_city_name(city_id):
    return CITY_MAPPING.get(city_id, f"City_{city_id}")

def get_category_name(cat_id):
    return CATEGORY_MAPPING.get(cat_id, f"Cat_{cat_id}")
