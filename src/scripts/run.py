#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: 启动脚本
@Version: 1.0
"""

from main.dask_cluster import init_cluster
from main.data_loader import load_data_dask, to_pandas
from main.preprocess import preprocess
from main.analysis import analyze
from main.visualize import plot_daily

if __name__ == "__main__":
    # 1. 启动 Dask 多核集群
    client = init_cluster()

    # 2. Dask 加载大数据
    df_dask = load_data_dask("example_large.csv")

    # 3. 数据预处理（仍然用 Dask）
    df_dask_clean = preprocess(df_dask)

    # 4. 转换为 pandas（用于深度分析与画图）
    df = to_pandas(df_dask_clean)

    # 5. 分析
    summary, daily = analyze(df)

    print(summary)
    print(daily.head())

    # 6. 绘图
    plot_daily(daily)



