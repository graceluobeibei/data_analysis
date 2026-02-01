#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: 测试pandas读取大文件，模拟OOM。 分配内存:1GB，文件大小:16GB。
@Version: 1.0
"""

import resource
import pandas as pd
import psutil
import sys
import platform

# 限制内存使用为1GB
limit = 1024 * 1024 * 1024  # 1GB bytes


def try_set_limit(resource_type, limit_value, description):
    """尝试设置指定的资源限制。"""
    try:
        soft, hard = resource.getrlimit(resource_type)
        print(f"当前 {description} 限制: soft={soft}, hard={hard}")
        
        # 尝试设置软限制，硬限制保持不变（如果可能）
        # 注意：在 macOS 上，降低硬限制可能会失败，或者设置软限制也可能失败
        target_hard = hard
        if hard == resource.RLIM_INFINITY:
             # 如果硬限制是无限，尝试将硬限制也设置为 limit，或者保持无限
             # 策略：先尝试 (limit, limit)
             try:
                 resource.setrlimit(resource_type, (limit_value, limit_value))
                 print(f"成功设置 {description} (soft=hard={limit_value})")
                 return True
             except ValueError:
                 pass
        
        # 如果上面的失败了，或者硬限制不是无限
        # 尝试 (limit, hard)
        resource.setrlimit(resource_type, (limit_value, hard))
        print(f"成功设置 {description} (soft={limit_value}, hard={hard})")
        return True
    except ValueError as e:
        print(f"设置 {description} 失败: {e}")
    except Exception as e:
        print(f"设置 {description} 时发生未知错误: {e}")
    return False

# 尝试设置内存限制
# 优先尝试 RLIMIT_AS (虚拟内存)，如果失败尝试 RLIMIT_DATA
limit_set = try_set_limit(resource.RLIMIT_AS, limit, "RLIMIT_AS")
if not limit_set:
    print("尝试设置 RLIMIT_DATA...")
    limit_set = try_set_limit(resource.RLIMIT_DATA, limit, "RLIMIT_DATA")

if not limit_set:
    print("\n警告: 无法在系统层面限制内存。脚本将继续运行，但可能无法模拟预期的 OOM 行为。", file=sys.stderr)
    print("在 macOS 上，这是常见问题。您可能需要依赖 Docker 或虚拟机来精确限制内存。", file=sys.stderr)
else:
    print(f"当前进程内存使用情况: {psutil.Process().memory_info().rss / 1024 / 1024}MB") 

try:
    # 读取大文件
    print("\n开始读取文件...")
    df = pd.read_csv('../../data/raw/D1_0.csv')
    print("文件读取成功！")
    print(df.head())
except MemoryError:
    print("内存不足，无法读取文件！(符合预期)", file=sys.stderr)
except Exception as e:
    print(f"读取文件时发生错误: {e}", file=sys.stderr)  
finally:
    print(f"最终进程内存使用情况: {psutil.Process().memory_info().rss / 1024 / 1024}MB") 
