"""
批量补全科创板（688）股票到 stocks 表，供下拉搜索使用。
默认不开启监控（is_monitor=0），避免一次拉入几百只进告警轮询。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import akshare as ak
import pandas as pd

from monitor.config.db_monitor import db_manager
from monitor.config.stock_code import normalize_monitor_stock_code

logger = logging.getLogger(__name__)


def _fetch_star_market_stocks() -> List[Tuple[str, str]]:
    """
    返回 [(688256.SH, 寒武纪), ...]
    """
    rows: List[Tuple[str, str]] = []
    seen = set()

    # 优先：上交所科创板名册
    try:
        df = ak.stock_info_sh_name_code(symbol="科创板")
        if isinstance(df, pd.DataFrame) and not df.empty:
            code_col = next((c for c in df.columns if "代码" in str(c) or str(c).lower() in {"code", "证券代码"}), None)
            name_col = next((c for c in df.columns if "名称" in str(c) or "简称" in str(c) or str(c).lower() in {"name", "证券简称"}), None)
            if code_col and name_col:
                for _, item in df.iterrows():
                    pure = str(item.get(code_col) or "").strip().zfill(6)
                    name = str(item.get(name_col) or "").strip()
                    if not pure.startswith("688"):
                        continue
                    ts_code = normalize_monitor_stock_code(pure, name)
                    if not ts_code or ts_code in seen:
                        continue
                    seen.add(ts_code)
                    rows.append((ts_code, name or ts_code))
    except Exception as exc:
        logger.warning("stock_info_sh_name_code(科创板) 失败: %s", exc)

    if rows:
        return rows

    # 回退：全 A 现货里筛 688
    try:
        df = ak.stock_zh_a_spot_em()
        if isinstance(df, pd.DataFrame) and not df.empty:
            code_col = "代码" if "代码" in df.columns else None
            name_col = "名称" if "名称" in df.columns else None
            if code_col and name_col:
                for _, item in df.iterrows():
                    pure = str(item.get(code_col) or "").strip().zfill(6)
                    name = str(item.get(name_col) or "").strip()
                    if not pure.startswith("688"):
                        continue
                    ts_code = normalize_monitor_stock_code(pure, name)
                    if not ts_code or ts_code in seen:
                        continue
                    seen.add(ts_code)
                    rows.append((ts_code, name or ts_code))
    except Exception as exc:
        logger.warning("stock_zh_a_spot_em 回退失败: %s", exc)

    return rows


def _existing_stock_codes() -> set:
    records = db_manager.execute_query("SELECT stock_code FROM stocks") or []
    codes = set()
    for item in records:
        code = normalize_monitor_stock_code(item.get("stock_code") or "")
        if code:
            codes.add(code)
        # 兼容历史前缀
        raw = str(item.get("stock_code") or "").strip()
        if raw:
            codes.add(raw)
            codes.add(raw.lower())
            codes.add(raw.upper())
    return codes


def sync_star_market_stocks(dry_run: bool = False) -> Dict[str, Any]:
    """
    批量插入缺失的科创板股票。
    """
    universe = _fetch_star_market_stocks()
    if not universe:
        return {
            "success": False,
            "message": "未获取到科创板股票列表（akshare 接口可能暂不可用）",
            "fetched": 0,
            "inserted": 0,
            "skipped": 0,
        }

    existing = _existing_stock_codes()
    to_insert: List[Tuple[str, str]] = []
    for ts_code, name in universe:
        aliases = {
            ts_code,
            ts_code.lower(),
            ts_code.upper(),
            f"sh{ts_code.split('.')[0]}",
            f"SH{ts_code.split('.')[0]}",
        }
        if aliases & existing:
            continue
        to_insert.append((ts_code, name))

    inserted = 0
    errors: List[str] = []
    if not dry_run:
        for ts_code, name in to_insert:
            try:
                row_id = db_manager.execute_insert(
                    "stocks",
                    {
                        "stock_code": ts_code,
                        "stock_name": name,
                        "is_monitor": 0,
                        "common": 0,
                        "normal_movement": 0,
                        "point_monitor_enabled": 0,
                        "point_monitor_mode": "both",
                        "divergence_enabled": 0,
                    },
                )
                if row_id:
                    inserted += 1
                    existing.add(ts_code)
                else:
                    errors.append(f"{ts_code} insert returned empty id")
            except Exception as exc:
                errors.append(f"{ts_code}: {exc}")
                logger.exception("插入科创板股票失败 %s", ts_code)
    else:
        inserted = len(to_insert)

    return {
        "success": True,
        "message": "dry-run 完成" if dry_run else f"已补全科创板，新增 {inserted} 只",
        "fetched": len(universe),
        "inserted": inserted,
        "skipped": len(universe) - len(to_insert),
        "pending": len(to_insert),
        "dry_run": dry_run,
        "sample_inserted": [f"{c} {n}" for c, n in to_insert[:10]],
        "errors": errors[:20],
    }
