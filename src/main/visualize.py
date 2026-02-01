#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: Dask 不能直接给 matplotlib，必须先 convert 为 pandas。
@Version: 1.0
"""

# matplotlib 画图
import matplotlib.pyplot as plt

def plot_daily(daily_series):
    plt.figure(figsize=(12, 5))
    plt.plot(daily_series.index, daily_series.values)
    plt.title("Daily Average Value")
    plt.xlabel("Date")
    plt.ylabel("Average Value")
    plt.tight_layout()
    plt.show()
    