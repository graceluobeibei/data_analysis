#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: Data loading utilities
@Version: 2.0
"""

import dask.dataframe as dd
import pandas as pd
import os
from .config import DATA_PATH, PARTITIONS, COLUMN_NAMES

def load_data_dask(filename: str):
    """Load large dataset using Dask."""
    path = os.path.join(DATA_PATH, filename)
    print(f"Loading with Dask: {path}")
    # Assuming CSV has no header based on column definition usage
    df = dd.read_csv(path, names=COLUMN_NAMES, header=None, blocksize="128MB")
    return df

def to_pandas(df_dask):
    """Convert Dask DataFrame to pandas (triggers computation)."""
    print("Converting Dask DataFrame to pandas…")
    return df_dask.compute()

def load_data_pandas(filename: str, sample_rows=None):
    """
    Load dataset using pandas directly (for smaller files or sampling).
    
    Args:
        filename: Name of the file in DATA_PATH
        sample_rows: If provided, only read this many rows (nrows)
    """
    path = os.path.join(DATA_PATH, filename)
    print(f"Loading with Pandas: {path}")
    
    try:
        if sample_rows:
            df = pd.read_csv(path, names=COLUMN_NAMES, header=None, nrows=sample_rows)
        else:
            df = pd.read_csv(path, names=COLUMN_NAMES, header=None)
        
        print(f"Loaded {len(df)} rows.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found at {path}")
        raise
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
