#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description:  TODO
@Version: 1.0
"""

import pandas as pd

def analyze(df: pd.DataFrame):
    summary = df.describe()

    # 示例：按日期聚合
    daily = df.groupby(df['date'].dt.date)["value"].mean()

    return summary, daily