#!/usr/bin/env python3
"""
@Author: Jupiter.Lin
@CreateDate: 2025-11-29
@Description: 使用 Dask 在受限内存环境下惰性读取超大 CSV 文件。
              目标: 在 2GB 进程内存上限下，使用 Dask 惰性读取 ~16GB CSV，
              仅做少量预览(head)，不做全量 compute，避免 OOM。
@Version: 1.3
"""

import os
import sys
import platform
from pathlib import Path
from time import perf_counter

import dask
import dask.dataframe as dd
import pandas as pd
import psutil
import resource


# 使用单线程调度器，避免在受限内存环境中创建过多线程
dask.config.set(scheduler="single-threaded")

# 强制禁用 PyArrow 后端，避免 Dask 自动推断触发依赖检查
try:
    pd.set_option("mode.dtype_backend", "numpy_nullable")
except Exception:
    pass
try:
    pd.set_option("mode.string_storage", "python")
except Exception:
    pass


# ---------------- 配置 ----------------
CURRENT_DIR = Path(__file__).parent
CSV_PATH = (CURRENT_DIR / "../../data/raw/D1_0.csv").resolve()
LIMIT_BYTES = 2 * 1024 * 1024 * 1024  # 2GB 内存上限
BLOCK_SIZE = "64MB"  # 较小的 Dask 分块大小
FORCE_COMPUTE = os.getenv("FORCE_COMPUTE", "0") in ("1", "true", "True")


# ---------------- 工具 ----------------
def fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.2f}{unit}"
        b /= 1024
    return f"{b:.2f}PB"


def rss_mb() -> float:
    return psutil.Process().memory_info().rss / 1024 / 1024


def set_memory_limit(limit_bytes: int) -> bool:
    """尽力将进程内存限制设为给定值；失败则返回 False。"""
    ok = False
    for rname in ("RLIMIT_AS", "RLIMIT_DATA"):
        if not hasattr(resource, rname):
            continue
        r = getattr(resource, rname)
        try:
            soft, hard = resource.getrlimit(r)
            if hard == resource.RLIM_INFINITY:
                try:
                    resource.setrlimit(r, (limit_bytes, limit_bytes))
                    ok = True
                    break
                except Exception:
                    pass
            try:
                resource.setrlimit(r, (limit_bytes, hard))
                ok = True
                break
            except Exception:
                pass
        except Exception:
            pass
    return ok


# ---------------- 主逻辑 ----------------
def safe_preview(df: dd.DataFrame) -> None:
    print("安全预览前 5 行(仅触发首分块读取，不会 OOM):")
    try:
        print(df.head())  # 仅读取少量数据
    except Exception as e:
        print(f"head 失败: {e}", file=sys.stderr)


def force_full_compute(df: dd.DataFrame) -> None:
    print("开始强制全量计算 (可能触发 OOM)...")
    t0 = perf_counter()
    try:
        pdf = df.compute()  # 将全部数据加载进内存, 这一步会触发 OOM
        t1 = perf_counter()
        print(f"compute 完成，耗时 {t1 - t0:.2f}s，行数={len(pdf)}")
        print("DataFrame 前 3 行:")
        print(pdf.head(3))
    except MemoryError:
        print("捕获 MemoryError：内存不足，已停止。", file=sys.stderr)
    except Exception as e:
        print(f"全量计算异常: {e}", file=sys.stderr)
    finally:
        print(f"当前 RSS: {rss_mb():.2f} MB")


def main() -> None:
    if not CSV_PATH.exists():
        print(f"文件不存在: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"平台: {platform.platform()}  Python: {platform.python_version()}")
    print(f"启动 RSS: {rss_mb():.2f} MB  计划内存上限: {fmt_bytes(LIMIT_BYTES)}")
    print(f"文件大小: {fmt_bytes(CSV_PATH.stat().st_size)}")

    if set_memory_limit(LIMIT_BYTES):
        print(f"已设置进程内存限制为 {fmt_bytes(LIMIT_BYTES)}。")
    else:
        print(
            "设置内存限制失败，建议使用 Docker --memory=1g 或 cgroups。",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"限制后 RSS: {rss_mb():.2f} MB")
    print(f"准备惰性读取文件: {CSV_PATH}")

    try:
        df = dd.read_csv(
            str(CSV_PATH),
            # compression="zip",     # 通常可不写，让 Dask 自动推断，最稳定的压缩文件格式是 .gz / .gzip（可流式读取)
            # blocksize=None,        # zip 难以高效分块，通常设为 None
            sep=",",
            encoding="utf-8",
            blocksize=BLOCK_SIZE,      # 使用可配置的小分块，降低单块内存消耗
            sample=256_000,            # 适中样本大小，避免 sample=False 的兼容性问题
            dtype=str,                 # 统一用字符串，避免复杂 dtype 推断
            assume_missing=True,
            on_bad_lines="skip",      # 跳过坏行
            engine="python",          # 用 Python 引擎，兼容性更好
            # 不要在这里加 usecols，如果要加，务必用列表，例如 usecols=list(range(10))
        )
        nparts = df.npartitions
        print(f"惰性 DataFrame 创建成功，分区数: {nparts}  (blocksize={BLOCK_SIZE})")
    except Exception as e:
        print("读取构建 Dask DataFrame 失败:", file=sys.stderr)
        print(f"type: {type(e)}", file=sys.stderr)
        print(f"repr: {repr(e)}", file=sys.stderr)
        print(f"str: {e}", file=sys.stderr)
        sys.exit(1)

    safe_preview(df)

    if FORCE_COMPUTE:
        print("检测到环境变量 FORCE_COMPUTE=1，将执行全量加载。")
        force_full_compute(df)
    else:
        print("未启用全量计算。设置 FORCE_COMPUTE=1 以模拟错误用法导致潜在 OOM。")

    print(f"结束 RSS: {rss_mb():.2f} MB")


if __name__ == "__main__":
    main()
