#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: 本地多核并行，也可以扩展成分布式
@Version: 1.0
"""


from dask.distributed import Client

def init_cluster():
    client = Client(
        n_workers=4,     # CPU 核数
        threads_per_worker=2,
        memory_limit="4GB"
    )
    print(client)
    return client
