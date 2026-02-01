#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: Data preprocessing
@Version: 2.0
"""

import pandas as pd
import dask.dataframe as dd
import numpy as np

def preprocess(df):
    """Legacy Dask preprocess function."""
    df = df.dropna()
    # df['date'] = dd.to_datetime(df['date']) # Old logic
    # df = df[df['value'] > 0] # Old logic
    return df

def preprocess_eleme_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the Ele.me recommendation dataset.
    
    1. Type conversion
    2. Missing value handling
    3. Feature extraction
    """
    print("Preprocessing data...")
    
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # 1. Ensure numeric types for critical columns
    numeric_cols = ['label', 'avg_price', 'ctr_30', 'ord_30', 'total_amt_30', 'rank_7', 'visit_city']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # 2. Handle Missing Values
    # Fill visit_city NaNs with 0 (Unknown)
    if 'visit_city' in df.columns:
        df['visit_city'] = df['visit_city'].fillna(0).astype(int)
        
    # Fill average price NaNs with mean
    if 'avg_price' in df.columns:
        mean_price = df['avg_price'].mean()
        df['avg_price'] = df['avg_price'].fillna(mean_price)
        
    # 3. Feature Extraction
    if 'times' in df.columns:
        # Convert Unix timestamp to datetime
        df['datetime'] = pd.to_datetime(df['times'], unit='s')
        
        # Verify/Overwrite hour and weekday if needed (using existing columns if accurate)
        # df['extracted_hour'] = df['datetime'].dt.hour
        # df['extracted_weekday'] = df['datetime'].dt.weekday
        
    # 4. Data Consistency
    # Ensure label is 0 or 1
    df = df[df['label'].isin([0, 1])]
    
    print("Preprocessing complete.")
    return df
