#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author: Jupiter.Lin
@Description: 将进程内存限制为 1GB，然后用 pandas 直接读取大 CSV，期望在受限环境触发 OOM。
@Usage: 请在Linux下运行: 
    python3 src/test/pandas_oom_simulator.py
    在 macOS 上，resource.setrlimit 可能不生效; 
    如需在MAC上模拟 OOM，请在 Docker 容器中运行，示例:
    docker run --rm -v $(pwd):/app -w /app --memory=1g python:3.9-slim python src/test/pandas_oom_simulator.py
"""
import sys
import platform
from pathlib import Path
import pandas as pd
import psutil
import resource

# 配置
CURRENT_DIR = Path(__file__).parent # 当前文件路径
CSV_PATH = Path(CURRENT_DIR / "../../data/raw/D1_0.csv")  # 16GB 文件路径: 相对于当前文件的路径
LIMIT_BYTES = 1024 * 1024 * 1024  # 1GB

# ---------------- 工具 ----------------
def fmt_bytes(b: int) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if b < 1024:
            return f"{b:.2f}{unit}"
        b /= 1024
    return f"{b:.2f}PB"

def set_1gb_limit_or_exit() -> None:
    """尽力将内存限制设为 1GB；失败则退出，保持简单行为。"""
    ok = False
    for rname in ("RLIMIT_AS", "RLIMIT_DATA"):
        if not hasattr(resource, rname):
            continue
        r = getattr(resource, rname)
        try:
            soft, hard = resource.getrlimit(r)
            if hard == resource.RLIM_INFINITY:
                try:
                    resource.setrlimit(r, (LIMIT_BYTES, LIMIT_BYTES))
                    ok = True
                    break
                except Exception:
                    pass
            # 尝试设置软限制，硬限制保持原值
            try:
                resource.setrlimit(r, (LIMIT_BYTES, hard))
                ok = True
                break
            except Exception:
                pass
        except Exception:
            pass
    if not ok:
        print("无法设置进程内存限制为 1GB。建议使用 Docker: --memory=1g。", file=sys.stderr)
        sys.exit(1)


def main():
    if not CSV_PATH.exists():
        print(f"文件不存在: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"平台: {platform.platform()}  Python: {platform.python_version()}")
    print(f"启动 RSS: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")

    set_1gb_limit_or_exit()
    print(f"限制后 RSS: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")
    print(f"文件大小: {fmt_bytes(CSV_PATH.stat().st_size)}")

    # 一次性读取，期望在 1GB 限制下 OOM
    try:
        print("开始一次性读取大 CSV (期待 OOM)...")
        df = pd.read_csv(CSV_PATH)
        print("读取成功(如果你没限制内存)！显示前5行：")
        print(df.head())
    except MemoryError:
        print("捕获 MemoryError：发生 OOM。", file=sys.stderr)
        # 打印出详细的异常信息以供调试
        print(sys.exc_info())
    except Exception as e:
        print(f"读取异常: {e}", file=sys.stderr)
    finally:
        print(f"实际内存使用: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
