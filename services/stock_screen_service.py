"""
市值 + 涨幅筛选：使用腾讯行情 qt.gtimg.cn（与前端日 K 的 fqkline 同属腾讯源），
批量拉取实时字段，避免东方财富分页接口易断连的问题。
"""
from __future__ import annotations

import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.dbconfig import exeQuery
from utils.common import format_stock_code

logger = logging.getLogger(__name__)

_GTIMG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.qq.com/",
}
_EM_HEADERS = {
    "User-Agent": _GTIMG_HEADERS["User-Agent"],
    "Referer": "https://quote.eastmoney.com/",
}

# 经样本校验：腾讯 v_sh600519 波浪线分段，下标从 0 起
# 31: 涨跌额, 32: 涨跌幅(%), 44: 总市值(亿元)
_IDX_NAME = 1
_IDX_PRICE = 3
_IDX_CHG_AMT = 31
_IDX_PCT = 32
_IDX_TURNOVER_RATE = 38
_IDX_TOTAL_MV_YI = 44
_IDX_FLOAT_MV_YI = 45

_EM_PROFILE_CACHE: Dict[str, Any] = {
    "expire_at": 0.0,
    "data": {},
}
_EM_PROFILE_CACHE_TTL_SECONDS = 20 * 60
_THS_HEADERS = {
    "User-Agent": _GTIMG_HEADERS["User-Agent"],
    "Referer": "https://basic.10jqka.com.cn/",
}
_THS_PROFILE_CACHE: Dict[str, Dict[str, Any]] = {}
_THS_PROFILE_ITEM_TTL_SECONDS = 6 * 60 * 60
_THS_PROFILE_FALLBACK_FETCH_CAP = 160
_THS_CONCEPT_MAX_ITEMS = 8


def _disable_proxy_for_requests() -> None:
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    for key in list(os.environ.keys()):
        if key.lower() in ("http_proxy", "https_proxy", "all_proxy"):
            os.environ.pop(key, None)


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def ts_to_gtimg_symbol(ts_code: str) -> Optional[str]:
    """600519.SH -> sh600519；920xxx.BJ -> bj920xxx"""
    ts_code = ts_code.strip().upper()
    if "." not in ts_code:
        return None
    num, suf = ts_code.split(".", 1)
    num = num.zfill(6)
    if suf == "SH":
        return f"sh{num}"
    if suf == "SZ":
        return f"sz{num}"
    if suf == "BJ":
        return f"bj{num}"
    return None


def _is_star_market_board(ts_code: str) -> bool:
    """科创板：证券代码 688 开头（沪市）。"""
    ts_code = ts_code.strip().upper()
    if "." not in ts_code:
        return False
    num, _ = ts_code.split(".", 1)
    return num.zfill(6).startswith("688")


def _prefix_to_ts_code(stock_code: str) -> Optional[str]:
    """
    sh600519/sz000001/600519 -> 600519.SH / 000001.SZ
    """
    if not stock_code:
        return None
    code = str(stock_code).strip().lower()
    if len(code) >= 8 and code[:2] in ("sh", "sz", "bj") and code[2:].isdigit():
        num = code[2:].zfill(6)
        return f"{num}.{code[:2].upper()}"
    if code.isdigit():
        num = code.zfill(6)
        if num.startswith("6"):
            return f"{num}.SH"
        return f"{num}.SZ"
    return None


def _load_universe_from_db() -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    try:
        records = exeQuery("SELECT stock_code, stock_name FROM stocks")
        if not records:
            return rows
        for item in records:
            ts_code = _prefix_to_ts_code(item.get("stock_code"))
            if not ts_code or _is_star_market_board(ts_code):
                continue
            sym = ts_to_gtimg_symbol(ts_code)
            if not sym:
                continue
            rows.append((sym, ts_code, (item.get("stock_name") or "").strip()))
    except Exception as e:
        logger.warning("从数据库加载股票池失败: %s", e)
    return rows


def _load_universe_from_eastmoney() -> List[Tuple[str, str, str]]:
    """
    通过东方财富公开接口拉取A股代码池（不依赖 akshare）。
    返回: [(gtimg_symbol, ts_code, name), ...]
    """
    rows: List[Tuple[str, str, str]] = []
    try:
        # 覆盖沪深主板/中小板/创业板/科创板等A股市场
        records = _load_eastmoney_clist_rows(fields="f12,f14")
        for item in records:
            code = str((item or {}).get("f12") or "").strip()
            name = str((item or {}).get("f14") or "").strip()
            if not code.isdigit() or len(code) > 6:
                continue
            code = code.zfill(6)
            market = "SH" if code.startswith("6") else "SZ"
            ts_code = f"{code}.{market}"
            if _is_star_market_board(ts_code):
                continue
            sym = ts_to_gtimg_symbol(ts_code)
            if not sym:
                continue
            rows.append((sym, ts_code, name))
    except Exception as e:
        logger.warning("从东方财富接口加载股票池失败: %s", e)
    return rows


def _load_eastmoney_clist_rows(fields: str, fid: str = "f12", max_pages: int = 40) -> List[dict]:
    """
    东方财富全A列表接口分页抓取（接口单页有上限，需翻页）。
    """
    out: List[dict] = []
    fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
    pz = 200
    session = requests.Session()
    session.trust_env = False
    _disable_proxy_for_requests()
    for pn in range(1, max_pages + 1):
        url = (
            "https://80.push2.eastmoney.com/api/qt/clist/get"
            f"?pn={pn}&pz={pz}&po=1&np=1&fltt=2&invt=2&fid={fid}"
            f"&fs={fs}&fields={fields}"
        )
        diff: List[dict] = []
        last_err: Optional[Exception] = None
        ok = False
        for attempt in range(3):
            try:
                resp = session.get(url, timeout=15, headers=_EM_HEADERS)
                resp.raise_for_status()
                data = resp.json()
                diff = (((data or {}).get("data") or {}).get("diff")) or []
                if not isinstance(diff, list):
                    diff = []
                ok = True
                break
            except Exception as e:
                last_err = e
                time.sleep(0.4 * (attempt + 1))
        if not ok:
            if out:
                logger.warning("东方财富 clist 分页中断 pn=%s（返回部分数据）: %s", pn, last_err)
                break
            logger.warning("东方财富 clist 拉取失败: %s", last_err)
            return out
        if len(diff) == 0:
            break
        out.extend(diff)
        if len(diff) < pz:
            break
        time.sleep(0.04)
    return out


def _load_em_profile_map() -> Dict[str, Dict[str, Optional[str]]]:
    """
    从东方财富批量获取股票画像信息：
    - f100: 行业（这里用于“板块”列）
    - f103: 概念（逗号分隔）
    """
    now = time.time()
    cached = _EM_PROFILE_CACHE
    cached_data = cached.get("data") if isinstance(cached.get("data"), dict) else {}
    if now < float(cached.get("expire_at", 0)) and isinstance(cached_data, dict):
        return cached.get("data") or {}

    profile_map: Dict[str, Dict[str, Optional[str]]] = {}
    try:
        rows = _load_eastmoney_clist_rows(fields="f12,f100,f103", fid="f12", max_pages=45)
        for item in rows:
            code = str((item or {}).get("f12") or "").strip()
            if not code.isdigit() or len(code) > 6:
                continue
            code = code.zfill(6)
            market = "SH" if code.startswith("6") else "SZ"
            ts_code = f"{code}.{market}"
            if _is_star_market_board(ts_code):
                continue

            board = str((item or {}).get("f100") or "").strip()
            concept = str((item or {}).get("f103") or "").strip()
            board = None if (not board or board == "-") else board
            concept = None if (not concept or concept == "-") else concept
            profile_map[ts_code] = {
                "board": board,
                "concept": concept,
            }
    except Exception as e:
        logger.warning("加载东方财富板块/概念信息失败: %s", e)
        profile_map = {}

    if profile_map:
        _EM_PROFILE_CACHE["data"] = profile_map
        _EM_PROFILE_CACHE["expire_at"] = time.time() + _EM_PROFILE_CACHE_TTL_SECONDS
        return profile_map

    # 拉取失败时，优先沿用旧缓存，避免整页突然全空
    if isinstance(cached_data, dict) and cached_data:
        _EM_PROFILE_CACHE["expire_at"] = time.time() + 120
        return cached_data

    # 首次即失败：仅短暂缓存空结果，允许快速重试
    _EM_PROFILE_CACHE["data"] = {}
    _EM_PROFILE_CACHE["expire_at"] = time.time() + 30
    return {}


def _clean_html_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "", raw or "")
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_ths_board(field_html: str) -> Optional[str]:
    m = re.search(
        r"三级行业分类：\s*<span[^>]*>(.*?)</span>",
        field_html or "",
        flags=re.I | re.S,
    )
    if not m:
        return None
    board = _clean_html_text(m.group(1))
    board = re.sub(r"[（(]\s*共\s*\d+\s*家\s*[)）]", "", board).strip()
    return board or None


def _parse_ths_concepts(concept_html: str) -> Optional[str]:
    blocks = re.findall(
        r"<td[^>]*class\s*=\s*[\"'][^\"']*gnName[^\"']*[\"'][^>]*>(.*?)</td>",
        concept_html or "",
        flags=re.I | re.S,
    )
    names: List[str] = []
    for block in blocks:
        name = _clean_html_text(block)
        if not name:
            continue
        if name in names:
            continue
        names.append(name)
        if len(names) >= _THS_CONCEPT_MAX_ITEMS:
            break
    return "，".join(names) if names else None


def _fetch_ths_profile(ts_code: str) -> Dict[str, Optional[str]]:
    code = str(ts_code or "").split(".")[0].strip()
    if not code.isdigit():
        return {"board": None, "concept": None}
    code = code.zfill(6)

    session = requests.Session()
    session.trust_env = False
    _disable_proxy_for_requests()

    board: Optional[str] = None
    concept: Optional[str] = None

    try:
        field_url = f"https://basic.10jqka.com.cn/{code}/field.html"
        resp = session.get(field_url, headers=_THS_HEADERS, timeout=12)
        resp.raise_for_status()
        resp.encoding = "gbk"
        board = _parse_ths_board(resp.text)
    except Exception:
        board = None

    try:
        concept_url = f"https://basic.10jqka.com.cn/{code}/concept.html"
        resp = session.get(concept_url, headers=_THS_HEADERS, timeout=12)
        resp.raise_for_status()
        resp.encoding = "gbk"
        concept = _parse_ths_concepts(resp.text)
    except Exception:
        concept = None

    return {"board": board, "concept": concept}


def _load_ths_profile_map(ts_codes: List[str], fetch_cap: int = _THS_PROFILE_FALLBACK_FETCH_CAP) -> Dict[str, Dict[str, Optional[str]]]:
    if not ts_codes:
        return {}

    now = time.time()
    out: Dict[str, Dict[str, Optional[str]]] = {}
    to_fetch: List[str] = []

    # 先用本进程缓存，避免重复抓取同一个股票。
    for ts_code in ts_codes:
        cache_item = _THS_PROFILE_CACHE.get(ts_code) or {}
        updated_at = float(cache_item.get("updated_at", 0) or 0)
        if now - updated_at < _THS_PROFILE_ITEM_TTL_SECONDS:
            out[ts_code] = {
                "board": cache_item.get("board"),
                "concept": cache_item.get("concept"),
            }
        else:
            to_fetch.append(ts_code)

    if not to_fetch:
        return out

    fetch_list = to_fetch[: max(1, int(fetch_cap))]
    max_workers = min(8, max(2, len(fetch_list)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ts = {executor.submit(_fetch_ths_profile, ts_code): ts_code for ts_code in fetch_list}
        for future in as_completed(future_to_ts):
            ts_code = future_to_ts[future]
            try:
                profile = future.result()
            except Exception:
                profile = {"board": None, "concept": None}
            _THS_PROFILE_CACHE[ts_code] = {
                "updated_at": time.time(),
                "board": profile.get("board"),
                "concept": profile.get("concept"),
            }
            out[ts_code] = profile

    return out


def _load_universe() -> List[Tuple[str, str, str]]:
    """
    (gtimg_symbol, ts_code, name_from_list)
    """
    rows: List[Tuple[str, str, str]] = []
    try:
        from utils import GetStockData

        result_map = getattr(GetStockData, "result", {}) or {}
        for ts_code, info in result_map.items():
            if _is_star_market_board(ts_code):
                continue
            sym = ts_to_gtimg_symbol(ts_code)
            if sym:
                rows.append((sym, ts_code, (info or {}).get("name") or ""))
        if rows:
            return rows
        logger.warning("GetStockData.result 为空，尝试回退到数据库 stocks 表")
    except Exception as e:
        logger.warning("加载全市场代码表失败: %s", e)

    db_rows = _load_universe_from_db()
    if db_rows:
        return db_rows

    em_rows = _load_universe_from_eastmoney()
    if em_rows:
        return em_rows

    return []


def _parse_gtimg_response(text: str) -> List[dict]:
    """解析 qt.gtimg.cn 返回的 v_sh600519=\"...\" 片段。"""
    out: List[dict] = []
    for m in re.finditer(r"v_(sh|sz|bj)(\d{6})=\"([^\"]*)\"", text, re.I):
        gsym = f"{m.group(1).lower()}{m.group(2)}"
        payload = m.group(3)
        parts = payload.split("~")
        if len(parts) <= _IDX_TOTAL_MV_YI:
            continue
        name = parts[_IDX_NAME] if len(parts) > _IDX_NAME else ""
        price = _safe_float(parts[_IDX_PRICE]) if len(parts) > _IDX_PRICE else None
        chg_amt = _safe_float(parts[_IDX_CHG_AMT]) if len(parts) > _IDX_CHG_AMT else None
        pct = _safe_float(parts[_IDX_PCT]) if len(parts) > _IDX_PCT else None
        turnover_rate = _safe_float(parts[_IDX_TURNOVER_RATE]) if len(parts) > _IDX_TURNOVER_RATE else None
        mv_yi = _safe_float(parts[_IDX_TOTAL_MV_YI])
        float_mv_yi = (
            _safe_float(parts[_IDX_FLOAT_MV_YI])
            if len(parts) > _IDX_FLOAT_MV_YI
            else None
        )
        out.append(
            {
                "gtimg_symbol": gsym,
                "name_raw": name,
                "price": price,
                "pct_chg": pct,
                "change_amount": chg_amt,
                "turnover_rate": turnover_rate,
                "total_mv_yi": mv_yi,
                "float_mv_yi": float_mv_yi,
            }
        )
    return out


def _fetch_gtimg_batches(symbols: List[str], batch_size: int = 80) -> List[dict]:
    _disable_proxy_for_requests()
    all_rows: List[dict] = []
    session = requests.Session()
    session.trust_env = False

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = session.get(url, headers=_GTIMG_HEADERS, timeout=25)
                r.raise_for_status()
                text = r.content.decode("gbk", errors="replace")
                all_rows.extend(_parse_gtimg_response(text))
                break
            except Exception as e:
                last_err = e
                time.sleep(0.6 * (2**attempt) + 0.1)
        else:
            logger.warning("腾讯行情批次失败 symbols=%s.. err=%s", batch[:3], last_err)
        time.sleep(0.06)
    return all_rows


def screen_stocks_by_mv_and_pct(
    min_mv_yi: float,
    min_pct_chg: float,
    limit: int = 3000,
) -> Tuple[List[dict], dict]:
    universe = _load_universe()
    if not universe:
        raise RuntimeError(
            "股票代码池为空（GetStockData/数据库stocks/东方财富接口均不可用）。"
            "请检查服务网络、数据库连接与启动日志。"
        )

    sym_to_meta = {u[0]: (u[1], u[2]) for u in universe}
    em_profile_map = _load_em_profile_map()
    symbols = list(sym_to_meta.keys())

    tz_cn = timezone(timedelta(hours=8))
    fetched_at = datetime.now(tz_cn).isoformat(timespec="seconds")

    quotes = _fetch_gtimg_batches(symbols)
    if not quotes:
        raise RuntimeError("腾讯行情接口未返回有效数据，请检查网络或稍后重试。")

    rows: List[dict] = []
    row_ts_codes: List[str] = []
    for q in quotes:
        gsym = q.get("gtimg_symbol")
        if not gsym or gsym not in sym_to_meta:
            continue
        ts_code, name_hint = sym_to_meta[gsym]
        if _is_star_market_board(ts_code):
            continue
        pure = ts_code.split(".")[0].zfill(6)
        try:
            stock_code = format_stock_code(pure, "prefix")
        except ValueError:
            stock_code = gsym

        mv_yi = q.get("total_mv_yi")
        pct = q.get("pct_chg")
        if mv_yi is None or pct is None:
            continue
        if mv_yi < float(min_mv_yi) or pct < float(min_pct_chg):
            continue

        nm = (q.get("name_raw") or "").strip() or name_hint
        fmv = q.get("float_mv_yi")
        float_mv_yi = round(fmv, 4) if fmv is not None else None
        profile = em_profile_map.get(ts_code) or {}
        board = profile.get("board")
        concept = profile.get("concept")
        turnover_rate = q.get("turnover_rate")

        rows.append(
            {
                "stock_code": stock_code,
                "stock_name": nm,
                "price": q.get("price"),
                "pct_chg": pct,
                "change_amount": q.get("change_amount"),
                "total_mv_yi": round(mv_yi, 4),
                "float_mv_yi": float_mv_yi,
                "turnover_rate": round(turnover_rate, 4) if turnover_rate is not None else None,
                "board": board,
                "concept": concept,
                "volume": None,
                "amount": None,
            }
        )
        row_ts_codes.append(ts_code)

    sorted_pairs = sorted(zip(rows, row_ts_codes), key=lambda pair: (pair[0].get("pct_chg") or 0), reverse=True)
    rows = [p[0] for p in sorted_pairs]
    row_ts_codes = [p[1] for p in sorted_pairs]
    if limit and limit > 0:
        rows = rows[: int(limit)]
        row_ts_codes = row_ts_codes[: int(limit)]

    em_profile_non_empty = 0
    for row in rows:
        if row.get("board") or row.get("concept"):
            em_profile_non_empty += 1

    ths_fallback_filled = 0
    missing_ts_codes = [
        ts_code
        for row, ts_code in zip(rows, row_ts_codes)
        if not row.get("board") or not row.get("concept")
    ]
    if missing_ts_codes:
        unique_missing_ts_codes = list(dict.fromkeys(missing_ts_codes))
        ths_profile_map = _load_ths_profile_map(unique_missing_ts_codes)
        for row, ts_code in zip(rows, row_ts_codes):
            profile = ths_profile_map.get(ts_code) or {}
            board_before = row.get("board")
            concept_before = row.get("concept")
            if not board_before:
                row["board"] = profile.get("board")
            if not concept_before:
                row["concept"] = profile.get("concept")
            if ((not board_before and row.get("board")) or (not concept_before and row.get("concept"))):
                ths_fallback_filled += 1

    meta = {
        "fetched_at": fetched_at,
        "source": "qt.gtimg.cn (Tencent, same vendor family as K-line fqkline)",
        "universe_size": len(symbols),
        "quotes_parsed": len(quotes),
        "total_after_filter": len(rows),
        "em_profile_non_empty": em_profile_non_empty,
        "ths_fallback_filled": ths_fallback_filled,
        "exclude_star_board": True,
        "exclude_note": "已排除科创板（代码 688 开头）",
    }
    return rows, meta
