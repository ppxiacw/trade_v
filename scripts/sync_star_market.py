"""命令行：批量补全科创板到 stocks 表。

用法:
  python scripts/sync_star_market.py
  python scripts/sync_star_market.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault('NO_PROXY', '*')
os.environ.setdefault('no_proxy', '*')

from utils.env_loader import load_local_env

load_local_env()

from services.star_market_sync_service import sync_star_market_stocks


def main():
    parser = argparse.ArgumentParser(description='批量补全科创板股票列表')
    parser.add_argument('--dry-run', action='store_true', help='只统计不写入')
    args = parser.parse_args()
    result = sync_star_market_stocks(dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get('success') else 1)


if __name__ == '__main__':
    main()
