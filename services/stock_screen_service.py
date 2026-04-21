"""
市值 + 涨幅筛选：使用腾讯行情 qt.gtimg.cn（与前端日 K 的 fqkline 同属腾讯源），
批量拉取实时字段，避免东方财富分页接口易断连的问题。
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone, timedelta
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
_THS_THEME_TREND_CACHE: Dict[str, Any] = {
    "expire_at": 0.0,
    "industry": {},
    "concept": {},
    "fetched_at": "",
    "source": "",
}
_THS_THEME_TREND_CACHE_TTL_SECONDS = 2 * 60
_EM_THEME_TREND_CACHE: Dict[str, Any] = {
    "expire_at": 0.0,
    "industry": {},
    "concept": {},
    "fetched_at": "",
}
_EM_THEME_TREND_CACHE_TTL_SECONDS = 2 * 60
_NOTICE_ITEM_CACHE: Dict[str, Dict[str, Any]] = {}
_NOTICE_ITEM_TTL_SECONDS = 30 * 60
_NOTICE_BATCH_SIZE = 20
_NOTICE_PAGE_SIZE = 100
_NOTICE_MAX_PAGES = 2
_FUTURE_EVENT_CACHE: Dict[str, Dict[str, Any]] = {}
_FUTURE_EVENT_ITEM_TTL_SECONDS = 60 * 60
_FUTURE_EVENT_FETCH_MAX_WORKERS = 6


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


def _safe_pct_float(x: Any) -> Optional[float]:
    if isinstance(x, str):
        normalized = x.strip().replace("%", "").replace(",", "")
        return _safe_float(normalized)
    return _safe_float(x)


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


def _format_notice_date(raw: Any) -> Optional[str]:
    text = str(raw or "").strip()
    if not text:
        return None
    if len(text) >= 10:
        return text[:10]
    return text or None


def _normalize_notice_title(title: str, short_name: str = "") -> str:
    text = str(title or "").strip()
    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)
    if "：" in text:
        prefix, rest = text.split("：", 1)
        if short_name and prefix.strip() == short_name.strip():
            text = rest.strip()
    elif ":" in text:
        prefix, rest = text.split(":", 1)
        if short_name and prefix.strip() == short_name.strip():
            text = rest.strip()

    return text.strip()


def _classify_notice_event(title: str, column_name: str) -> Tuple[int, str]:
    text = f"{column_name} {title}"

    if any(keyword in text for keyword in ("年度报告", "年报", "半年度报告", "半年报", "季报", "一季报", "三季报")):
        return 100, "财报"
    if any(keyword in text for keyword in ("业绩预告", "业绩快报", "业绩说明会", "业绩")):
        return 95, "业绩"
    if any(keyword in text for keyword in ("分红", "利润分配", "权益分派", "派息")):
        return 88, "分红"
    if any(keyword in text for keyword in ("回购", "增持", "减持")):
        return 82, "回购"
    if any(keyword in text for keyword in ("重组", "并购", "收购", "重大资产")):
        return 78, "重组"
    if any(keyword in text for keyword in ("停牌", "复牌", "异常波动", "风险提示")):
        return 74, "事项"
    if any(keyword in text for keyword in ("投资者关系", "活动记录", "调研")):
        return 40, "互动"

    column_clean = str(column_name or "").strip()
    return 55, column_clean or "公告"


def _build_notice_item_payload(item: dict) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    codes = item.get("codes") or []
    code_info = codes[0] if isinstance(codes, list) and codes else {}
    short_name = str((code_info or {}).get("short_name") or "").strip()
    title_raw = str(item.get("title_ch") or item.get("title") or "").strip()
    title = _normalize_notice_title(title_raw, short_name)
    if not title:
        return None

    column_info = (item.get("columns") or [{}])[0] or {}
    column_name = str(column_info.get("column_name") or "").strip()
    notice_date = _format_notice_date(item.get("notice_date") or item.get("sort_date"))
    score, event_type = _classify_notice_event(title, column_name)
    sort_key = str(item.get("sort_date") or item.get("notice_date") or "")

    return {
        "latest_event_date": notice_date,
        "latest_event_title": title,
        "latest_event_type": event_type,
        "_score": score,
        "_sort_key": sort_key,
    }


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


def _normalize_theme_name(name: Any) -> str:
    text = str(name or "").strip().lower()
    if not text:
        return ""
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("－", "-").replace("—", "-")
    text = re.sub(r"\s+", "", text)
    return text


def _pick_first_existing_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _load_eastmoney_theme_trend_maps() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    东方财富板块/概念涨跌榜兜底：
    - 行业: fs=m:90+t:2
    - 概念: fs=m:90+t:3
    """

    now = time.time()
    cached = _EM_THEME_TREND_CACHE
    if (
        now < float(cached.get("expire_at", 0) or 0)
        and isinstance(cached.get("industry"), dict)
        and isinstance(cached.get("concept"), dict)
    ):
        return cached.get("industry") or {}, cached.get("concept") or {}

    def fetch_theme_map(fs: str, theme_type: str) -> Dict[str, Dict[str, Any]]:
        session = requests.Session()
        session.trust_env = False
        _disable_proxy_for_requests()

        out: Dict[str, Dict[str, Any]] = {}
        page_size = 200
        for page_no in range(1, 16):
            url = (
                "https://79.push2.eastmoney.com/api/qt/clist/get"
                f"?pn={page_no}&pz={page_size}&po=1&np=1&fltt=2&invt=2&fid=f3"
                f"&fs={fs}&fields=f12,f14,f3"
            )
            try:
                resp = session.get(url, timeout=15, headers=_EM_HEADERS)
                resp.raise_for_status()
                payload = resp.json()
                diff = (((payload or {}).get("data") or {}).get("diff")) or []
                if not isinstance(diff, list):
                    diff = []
            except Exception:
                break

            if not diff:
                break

            for item in diff:
                theme_name = str((item or {}).get("f14") or "").strip()
                if not theme_name:
                    continue
                normalized_name = _normalize_theme_name(theme_name)
                if not normalized_name:
                    continue
                pct_value = _safe_pct_float((item or {}).get("f3"))
                index_code = str((item or {}).get("f12") or "").strip()
                out[normalized_name] = {
                    "name": theme_name,
                    "pct_chg": pct_value,
                    "index_code": index_code or None,
                    "lead_stock": None,
                    "theme_type": theme_type,
                }

            if len(diff) < page_size:
                break
            time.sleep(0.03)
        return out

    industry_map = fetch_theme_map("m:90+t:2", "industry")
    concept_map = fetch_theme_map("m:90+t:3", "concept")
    if industry_map or concept_map:
        _EM_THEME_TREND_CACHE["industry"] = industry_map
        _EM_THEME_TREND_CACHE["concept"] = concept_map
        _EM_THEME_TREND_CACHE["fetched_at"] = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        _EM_THEME_TREND_CACHE["expire_at"] = time.time() + _EM_THEME_TREND_CACHE_TTL_SECONDS
        return industry_map, concept_map

    cached_industry = cached.get("industry") if isinstance(cached.get("industry"), dict) else {}
    cached_concept = cached.get("concept") if isinstance(cached.get("concept"), dict) else {}
    if cached_industry or cached_concept:
        _EM_THEME_TREND_CACHE["expire_at"] = time.time() + 30
        return cached_industry, cached_concept
    return {}, {}


def _load_ths_theme_trend_maps() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], str, str]:
    now = time.time()
    cached = _THS_THEME_TREND_CACHE
    if (
        now < float(cached.get("expire_at", 0) or 0)
        and isinstance(cached.get("industry"), dict)
        and isinstance(cached.get("concept"), dict)
    ):
        return (
            cached.get("industry") or {},
            cached.get("concept") or {},
            str(cached.get("fetched_at") or ""),
            str(cached.get("source") or ""),
        )

    industry_map: Dict[str, Dict[str, Any]] = {}
    concept_map: Dict[str, Dict[str, Any]] = {}
    fetched_at = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    source = ""

    try:
        import akshare as ak

        industry_df = ak.stock_board_industry_name_ths()
        concept_df = ak.stock_board_concept_name_ths()

        def build_map(df: Any, is_concept: bool) -> Dict[str, Dict[str, Any]]:
            out: Dict[str, Dict[str, Any]] = {}
            if df is None or getattr(df, "empty", True):
                return out
            columns = [str(col) for col in list(getattr(df, "columns", []))]
            name_col = _pick_first_existing_column(
                columns,
                ["行业", "概念名称", "概念", "板块", "名称"],
            )
            pct_col = _pick_first_existing_column(columns, ["涨跌幅", "涨跌幅(%)", "涨幅", "最新涨跌幅"])
            index_col = _pick_first_existing_column(columns, ["行业指数", "概念指数", "指数代码", "代码"])
            lead_col = _pick_first_existing_column(columns, ["领涨股票", "领涨股", "领涨个股"])
            if not name_col:
                return out

            for item in df.to_dict(orient="records"):
                theme_name = str((item or {}).get(name_col) or "").strip()
                if not theme_name:
                    continue
                normalized_name = _normalize_theme_name(theme_name)
                if not normalized_name:
                    continue
                pct_value = _safe_pct_float((item or {}).get(pct_col)) if pct_col else None
                index_code = str((item or {}).get(index_col) or "").strip() if index_col else ""
                lead_stock = str((item or {}).get(lead_col) or "").strip() if lead_col else ""
                out[normalized_name] = {
                    "name": theme_name,
                    "pct_chg": pct_value,
                    "index_code": index_code or None,
                    "lead_stock": lead_stock or None,
                    "theme_type": "concept" if is_concept else "industry",
                }
            return out

        industry_map = build_map(industry_df, is_concept=False)
        concept_map = build_map(concept_df, is_concept=True)
        if industry_map or concept_map:
            source = "ths_akshare"
    except Exception as e:
        logger.warning("加载同花顺板块/概念走势失败: %s", e)

    if not industry_map and not concept_map:
        try:
            industry_map, concept_map = _load_eastmoney_theme_trend_maps()
            if industry_map or concept_map:
                source = "eastmoney_fallback"
        except Exception as e:
            logger.warning("东方财富板块/概念走势兜底失败: %s", e)

    if industry_map or concept_map:
        _THS_THEME_TREND_CACHE["industry"] = industry_map
        _THS_THEME_TREND_CACHE["concept"] = concept_map
        _THS_THEME_TREND_CACHE["fetched_at"] = fetched_at
        _THS_THEME_TREND_CACHE["source"] = source
        _THS_THEME_TREND_CACHE["expire_at"] = time.time() + _THS_THEME_TREND_CACHE_TTL_SECONDS
        return industry_map, concept_map, fetched_at, source

    # 拉取失败时沿用旧缓存，避免页面突然空白。
    cached_industry = cached.get("industry") if isinstance(cached.get("industry"), dict) else {}
    cached_concept = cached.get("concept") if isinstance(cached.get("concept"), dict) else {}
    if cached_industry or cached_concept:
        _THS_THEME_TREND_CACHE["expire_at"] = time.time() + 30
        return (
            cached_industry,
            cached_concept,
            str(cached.get("fetched_at") or fetched_at),
            str(cached.get("source") or ""),
        )

    _THS_THEME_TREND_CACHE["industry"] = {}
    _THS_THEME_TREND_CACHE["concept"] = {}
    _THS_THEME_TREND_CACHE["fetched_at"] = fetched_at
    _THS_THEME_TREND_CACHE["source"] = ""
    _THS_THEME_TREND_CACHE["expire_at"] = time.time() + 20
    return {}, {}, fetched_at, ""


def _split_board_candidates(board_text: Any) -> List[str]:
    text = str(board_text or "").strip()
    if not text:
        return []
    parts = [seg.strip() for seg in re.split(r"\s*--\s*|[>/｜|]+", text) if seg and seg.strip()]
    candidates: List[str] = []
    for part in reversed(parts):
        if part not in candidates:
            candidates.append(part)
    if text not in candidates:
        candidates.append(text)
    return candidates


def _split_concept_candidates(concept_text: Any) -> List[str]:
    text = str(concept_text or "").strip()
    if not text:
        return []
    parts = [seg.strip() for seg in re.split(r"[，,、/|；;]+", text) if seg and seg.strip()]
    candidates: List[str] = []
    for part in parts:
        if part not in candidates:
            candidates.append(part)
    if text not in candidates:
        candidates.append(text)
    return candidates


def _pick_theme_trend(candidates: List[str], trend_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for candidate in candidates:
        matched = trend_map.get(_normalize_theme_name(candidate))
        if matched:
            return matched
    return None


def _enrich_rows_with_ths_theme_trend(rows: List[dict]) -> Dict[str, Any]:
    for row in rows:
        row["board_trend_name"] = None
        row["board_trend_pct"] = None
        row["board_trend_index_code"] = None
        row["board_trend_lead_stock"] = None
        row["concept_trend_name"] = None
        row["concept_trend_pct"] = None
        row["concept_trend_index_code"] = None
        row["concept_trend_lead_stock"] = None

    if not rows:
        return {
            "enabled": False,
            "board_matched": 0,
            "concept_matched": 0,
            "fetched_at": "",
            "source": "",
        }

    industry_map, concept_map, fetched_at, source = _load_ths_theme_trend_maps()
    if not industry_map and not concept_map:
        return {
            "enabled": False,
            "board_matched": 0,
            "concept_matched": 0,
            "fetched_at": fetched_at,
            "source": source,
        }

    board_matched = 0
    concept_matched = 0
    for row in rows:
        board_match = _pick_theme_trend(_split_board_candidates(row.get("board")), industry_map)
        if board_match:
            row["board_trend_name"] = board_match.get("name")
            row["board_trend_pct"] = board_match.get("pct_chg")
            row["board_trend_index_code"] = board_match.get("index_code")
            row["board_trend_lead_stock"] = board_match.get("lead_stock")
            board_matched += 1

        concept_match = _pick_theme_trend(_split_concept_candidates(row.get("concept")), concept_map)
        if concept_match:
            row["concept_trend_name"] = concept_match.get("name")
            row["concept_trend_pct"] = concept_match.get("pct_chg")
            row["concept_trend_index_code"] = concept_match.get("index_code")
            row["concept_trend_lead_stock"] = concept_match.get("lead_stock")
            concept_matched += 1

    return {
        "enabled": True,
        "board_matched": board_matched,
        "concept_matched": concept_matched,
        "fetched_at": fetched_at,
        "source": source,
    }


def _normalize_theme_type(theme_type: Any) -> str:
    raw = str(theme_type or "").strip().lower()
    if raw in {"industry", "board", "hy"}:
        return "industry"
    if raw in {"concept", "gn"}:
        return "concept"
    return "industry"


def _normalize_theme_code(theme_code: Any) -> str:
    code = str(theme_code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        suffix = code.split(".")[-1].strip().upper()
        if suffix.startswith("BK"):
            return suffix
    return code


def _resolve_theme_index_code(theme_type: str, theme_code: str, theme_name: str) -> str:
    normalized_code = _normalize_theme_code(theme_code)
    if normalized_code.startswith("BK"):
        return normalized_code

    target_map = {}
    industry_map, concept_map, _, _ = _load_ths_theme_trend_maps()
    if theme_type == "concept":
        target_map = concept_map
    else:
        target_map = industry_map

    normalized_name = _normalize_theme_name(theme_name)
    if normalized_name and normalized_name in target_map:
        candidate = _normalize_theme_code(target_map[normalized_name].get("index_code"))
        if candidate.startswith("BK"):
            return candidate

    em_industry_map, em_concept_map = _load_eastmoney_theme_trend_maps()
    em_map = em_concept_map if theme_type == "concept" else em_industry_map
    if normalized_name and normalized_name in em_map:
        candidate = _normalize_theme_code(em_map[normalized_name].get("index_code"))
        if candidate.startswith("BK"):
            return candidate

    if normalized_code.startswith("BK"):
        return normalized_code
    raise ValueError("未找到可用的板块/概念指数代码")


def _fetch_theme_kline_rows_from_eastmoney(index_code: str, period: str, limit: int) -> Tuple[List[dict], Dict[str, Any]]:
    period_to_klt = {
        "time": "1",
        "m1": "1",
        "m5": "5",
        "m15": "15",
        "m30": "30",
        "day": "101",
        "week": "102",
        "month": "103",
    }
    klt = period_to_klt.get(period, "101")
    secid = f"90.{index_code}"
    lmt = max(20, min(int(limit or 240), 800))

    session = requests.Session()
    session.trust_env = False
    _disable_proxy_for_requests()
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": klt,
        "fqt": "1",
        "lmt": str(lmt),
        "end": "20500101",
    }
    resp = session.get(
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        params=params,
        headers=_EM_HEADERS,
        timeout=18,
    )
    resp.raise_for_status()
    payload = resp.json()
    data = (payload or {}).get("data") or {}
    if not data:
        return [], {"theme_name": "", "index_code": index_code, "source": "eastmoney_push2his"}

    out: List[dict] = []
    for row in data.get("klines") or []:
        parts = str(row or "").split(",")
        if len(parts) < 6:
            continue
        open_price = _safe_float(parts[1])
        close_price = _safe_float(parts[2])
        high_price = _safe_float(parts[3])
        low_price = _safe_float(parts[4])
        volume = _safe_float(parts[5])
        amount = _safe_float(parts[6]) if len(parts) > 6 else None
        pct_chg = _safe_pct_float(parts[8]) if len(parts) > 8 else None
        if None in (open_price, close_price, high_price, low_price):
            continue
        out.append(
            {
                "date": parts[0],
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "amount": amount,
                "pct_chg": pct_chg,
            }
        )

    return out, {
        "theme_name": str(data.get("name") or "").strip(),
        "index_code": str(data.get("code") or index_code).strip(),
        "source": "eastmoney_push2his",
    }


def load_theme_kline_data(
    theme_type: str,
    theme_code: str = "",
    theme_name: str = "",
    period: str = "day",
    limit: int = 240,
) -> Tuple[List[dict], Dict[str, Any]]:
    normalized_type = _normalize_theme_type(theme_type)
    normalized_period = str(period or "day").strip().lower()
    if normalized_period not in {"time", "m1", "m5", "m15", "m30", "day", "week", "month"}:
        normalized_period = "day"

    index_code = _resolve_theme_index_code(normalized_type, theme_code, theme_name)
    rows, extra_meta = _fetch_theme_kline_rows_from_eastmoney(index_code, normalized_period, limit)
    meta = {
        "theme_type": normalized_type,
        "theme_code": index_code,
        "theme_name": extra_meta.get("theme_name") or str(theme_name or "").strip(),
        "period": normalized_period,
        "count": len(rows),
        "source": extra_meta.get("source") or "eastmoney_push2his",
        "fetched_at": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
    }
    return rows, meta


def _fetch_notice_batch(pure_codes: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    if not pure_codes:
        return {}

    expected = {str(code or "").strip().zfill(6) for code in pure_codes if str(code or "").strip()}
    if not expected:
        return {}

    fallback_map: Dict[str, Dict[str, Any]] = {}
    best_map: Dict[str, Dict[str, Any]] = {}

    session = requests.Session()
    session.trust_env = False
    _disable_proxy_for_requests()

    for page_index in range(1, _NOTICE_MAX_PAGES + 1):
        params = {
            "ann_type": "A",
            "client_source": "web",
            "page_index": page_index,
            "page_size": _NOTICE_PAGE_SIZE,
            "sr": -1,
            "stock_list": ",".join(sorted(expected)),
        }

        notice_list: List[dict] = []
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                resp = session.get(
                    "https://np-anotice-stock.eastmoney.com/api/security/ann",
                    params=params,
                    headers=_EM_HEADERS,
                    timeout=18,
                )
                resp.raise_for_status()
                data = resp.json()
                notice_list = (((data or {}).get("data") or {}).get("list")) or []
                if not isinstance(notice_list, list):
                    notice_list = []
                break
            except Exception as e:
                last_err = e
                time.sleep(0.4 * (attempt + 1))
        else:
            logger.warning("东方财富公告批量拉取失败 codes=%s err=%s", list(expected)[:3], last_err)
            break

        if not notice_list:
            break

        for item in notice_list:
            payload = _build_notice_item_payload(item)
            if not payload:
                continue
            for code_info in item.get("codes") or []:
                pure_code = str((code_info or {}).get("stock_code") or "").strip().zfill(6)
                if pure_code not in expected:
                    continue
                fallback_map.setdefault(pure_code, payload)
                current = best_map.get(pure_code)
                if current is None:
                    best_map[pure_code] = payload
                    continue
                if payload["_score"] > current["_score"]:
                    best_map[pure_code] = payload
                    continue
                if payload["_score"] == current["_score"] and payload["_sort_key"] > current["_sort_key"]:
                    best_map[pure_code] = payload

        if expected.issubset(fallback_map.keys()):
            all_priority_enough = all(
                (best_map.get(pure_code) or {}).get("_score", 0) >= 82 for pure_code in expected
            )
            if all_priority_enough:
                break

    result: Dict[str, Dict[str, Optional[str]]] = {}
    for pure_code in expected:
        item = best_map.get(pure_code) or fallback_map.get(pure_code) or {}
        result[pure_code] = {
            "latest_event_date": item.get("latest_event_date"),
            "latest_event_title": item.get("latest_event_title"),
            "latest_event_type": item.get("latest_event_type"),
        }
    return result


def _load_latest_notice_map(ts_codes: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
    if not ts_codes:
        return {}

    now = time.time()
    result: Dict[str, Dict[str, Optional[str]]] = {}
    pending_pure_codes: List[str] = []
    ts_code_to_pure_code: Dict[str, str] = {}

    for ts_code in list(dict.fromkeys(ts_codes)):
        pure_code = str(ts_code or "").split(".")[0].strip().zfill(6)
        if not pure_code:
            continue
        ts_code_to_pure_code[ts_code] = pure_code
        cache_item = _NOTICE_ITEM_CACHE.get(pure_code) or {}
        updated_at = float(cache_item.get("updated_at", 0) or 0)
        if now - updated_at < _NOTICE_ITEM_TTL_SECONDS:
            result[ts_code] = {
                "latest_event_date": cache_item.get("latest_event_date"),
                "latest_event_title": cache_item.get("latest_event_title"),
                "latest_event_type": cache_item.get("latest_event_type"),
            }
        else:
            pending_pure_codes.append(pure_code)

    unique_pending = list(dict.fromkeys(pending_pure_codes))
    if unique_pending:
        batches = [
            unique_pending[i : i + _NOTICE_BATCH_SIZE]
            for i in range(0, len(unique_pending), _NOTICE_BATCH_SIZE)
        ]
        max_workers = min(4, max(1, len(batches)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(_fetch_notice_batch, batch): batch for batch in batches
            }
            for future in as_completed(future_to_batch):
                batch_result = future.result() or {}
                fetched_at = time.time()
                for pure_code in future_to_batch[future]:
                    payload = batch_result.get(pure_code) or {
                        "latest_event_date": None,
                        "latest_event_title": None,
                        "latest_event_type": None,
                    }
                    _NOTICE_ITEM_CACHE[pure_code] = {
                        "updated_at": fetched_at,
                        "latest_event_date": payload.get("latest_event_date"),
                        "latest_event_title": payload.get("latest_event_title"),
                        "latest_event_type": payload.get("latest_event_type"),
                    }

    for ts_code, pure_code in ts_code_to_pure_code.items():
        if ts_code in result:
            continue
        cache_item = _NOTICE_ITEM_CACHE.get(pure_code) or {}
        result[ts_code] = {
            "latest_event_date": cache_item.get("latest_event_date"),
            "latest_event_title": cache_item.get("latest_event_title"),
            "latest_event_type": cache_item.get("latest_event_type"),
        }

    return result


def _stock_code_to_pure_code(stock_code: str) -> Optional[str]:
    code = str(stock_code or "").strip()
    if not code:
        return None

    prefix_match = re.fullmatch(r"(sh|sz|bj)(\d{1,6})", code, flags=re.I)
    if prefix_match:
        return str(prefix_match.group(2) or "").zfill(6)

    suffix_match = re.fullmatch(r"(\d{1,6})\.(SH|SZ|BJ)", code, flags=re.I)
    if suffix_match:
        return str(suffix_match.group(1) or "").zfill(6)

    if code.isdigit():
        return code.zfill(6)

    return None


def _pure_code_to_prefix_code(pure_code: str) -> str:
    pure = str(pure_code or "").strip().zfill(6)
    if pure.startswith(("8", "4")):
        return f"bj{pure}"
    if pure.startswith("6"):
        return f"sh{pure}"
    return f"sz{pure}"


def _extract_json_object_after_marker(text: str, marker: str) -> Optional[str]:
    if not text or not marker:
        return None

    marker_index = text.find(marker)
    if marker_index < 0:
        return None

    start = text.find("{", marker_index)
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]

    return None


def _normalize_future_event_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _score_future_event(event_type: str, content: str) -> int:
    text = f"{event_type} {content}"

    if any(keyword in text for keyword in ("预约披露", "年报", "半年报", "季报", "一季报", "三季报", "定期报告")):
        return 100
    if any(keyword in text for keyword in ("业绩预告", "业绩快报", "业绩说明会", "业绩")):
        return 95
    if any(keyword in text for keyword in ("股东大会", "董事会", "监事会")):
        return 88
    if any(keyword in text for keyword in ("解禁", "限售")):
        return 86
    if any(keyword in text for keyword in ("分红", "派息", "权益分派", "利润分配", "送转")):
        return 84
    if any(keyword in text for keyword in ("回购", "增持", "减持")):
        return 82
    if any(keyword in text for keyword in ("重组", "并购", "收购", "重大资产")):
        return 80
    if any(keyword in text for keyword in ("停牌", "复牌", "异常波动", "风险提示")):
        return 76
    if any(keyword in text for keyword in ("公告", "提示性公告")):
        return 64
    if any(keyword in text for keyword in ("调研", "投资者关系", "活动记录", "研报")):
        return 48
    if any(keyword in text for keyword in ("融资融券", "大宗交易", "股权质押")):
        return 20
    return 52


def _build_future_event_payload(item: dict, today_str: str) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    event_date = _format_notice_date(item.get("NOTICE_DATE"))
    if not event_date or event_date < today_str:
        return None

    event_type = _normalize_future_event_text(item.get("EVENT_TYPE") or "")
    event_title = _normalize_future_event_text(item.get("LEVEL1_CONTENT") or "")
    if not event_type and not event_title:
        return None
    if not event_title:
        event_title = event_type
    if not event_type:
        event_type = "事项"

    return {
        "event_date": event_date,
        "event_type": event_type,
        "event_title": event_title,
        "_score": _score_future_event(event_type, event_title),
    }


def _fetch_future_events_for_pure_code(pure_code: str) -> List[Dict[str, str]]:
    pure = str(pure_code or "").strip().zfill(6)
    if not pure.isdigit():
        return []

    session = requests.Session()
    session.trust_env = False
    _disable_proxy_for_requests()

    try:
        resp = session.get(
            f"https://data.eastmoney.com/stockcalendar/{pure}.html",
            headers=_EM_HEADERS,
            timeout=18,
        )
        resp.raise_for_status()
        html = resp.content.decode("utf-8", errors="replace")
        json_text = _extract_json_object_after_marker(html, "var pagedata =")
        if not json_text:
            return []
        page_data = json.loads(json_text)
        rows = ((((page_data or {}).get("sjyl") or {}).get("result") or {}).get("data")) or []
        if not isinstance(rows, list):
            return []
    except Exception as e:
        logger.warning("东方财富个股日历拉取失败 stock=%s err=%s", pure, e)
        return []

    tz_cn = timezone(timedelta(hours=8))
    today_str = datetime.now(tz_cn).strftime("%Y-%m-%d")
    events: List[Dict[str, Any]] = []
    seen = set()
    for item in rows:
        payload = _build_future_event_payload(item, today_str)
        if not payload:
            continue
        dedupe_key = (
            payload.get("event_date"),
            payload.get("event_type"),
            payload.get("event_title"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        events.append(payload)

    events.sort(
        key=lambda item: (
            item.get("event_date") or "9999-12-31",
            -int(item.get("_score", 0) or 0),
            str(item.get("event_type") or ""),
            str(item.get("event_title") or ""),
        )
    )

    return [
        {
            "event_date": str(item.get("event_date") or ""),
            "event_type": str(item.get("event_type") or ""),
            "event_title": str(item.get("event_title") or ""),
        }
        for item in events
    ]


def load_future_events_by_stock_codes(stock_codes: List[str]) -> Tuple[Dict[str, List[Dict[str, str]]], dict]:
    if not stock_codes:
        return {}, {"requested": 0, "filled_codes": 0, "total_events": 0}

    now = time.time()
    key_to_pure: Dict[str, str] = {}
    pure_to_keys: Dict[str, List[str]] = {}
    for stock_code in stock_codes:
        pure_code = _stock_code_to_pure_code(str(stock_code or ""))
        if not pure_code:
            continue
        key = _pure_code_to_prefix_code(pure_code)
        key_to_pure[key] = pure_code
        pure_to_keys.setdefault(pure_code, [])
        if key not in pure_to_keys[pure_code]:
            pure_to_keys[pure_code].append(key)

    result_by_pure: Dict[str, List[Dict[str, str]]] = {}
    pending_pure_codes: List[str] = []
    for pure_code in pure_to_keys.keys():
        cache_item = _FUTURE_EVENT_CACHE.get(pure_code) or {}
        updated_at = float(cache_item.get("updated_at", 0) or 0)
        if now - updated_at < _FUTURE_EVENT_ITEM_TTL_SECONDS:
            cached_events = cache_item.get("events")
            result_by_pure[pure_code] = list(cached_events or [])
        else:
            pending_pure_codes.append(pure_code)

    if pending_pure_codes:
        max_workers = min(_FUTURE_EVENT_FETCH_MAX_WORKERS, max(1, len(pending_pure_codes)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {
                executor.submit(_fetch_future_events_for_pure_code, pure_code): pure_code
                for pure_code in pending_pure_codes
            }
            for future in as_completed(future_to_code):
                pure_code = future_to_code[future]
                try:
                    events = future.result() or []
                except Exception as e:
                    logger.warning("未来事件任务失败 stock=%s err=%s", pure_code, e)
                    events = []
                result_by_pure[pure_code] = list(events)
                _FUTURE_EVENT_CACHE[pure_code] = {
                    "updated_at": time.time(),
                    "events": list(events),
                }

    data: Dict[str, List[Dict[str, str]]] = {}
    filled_codes = 0
    total_events = 0
    for pure_code, keys in pure_to_keys.items():
        events = list(result_by_pure.get(pure_code) or [])
        if events:
            filled_codes += len(keys)
            total_events += len(events) * len(keys)
        for key in keys:
            data[key] = list(events)

    tz_cn = timezone(timedelta(hours=8))
    return data, {
        "requested": len(key_to_pure),
        "filled_codes": filled_codes,
        "total_events": total_events,
        "from_date": datetime.now(tz_cn).strftime("%Y-%m-%d"),
        "source": "Eastmoney stock calendar",
    }


def _load_universe() -> List[Tuple[str, str, str]]:
    """
    (gtimg_symbol, ts_code, name_from_list)
    """
    rows: List[Tuple[str, str, str]] = []
    try:
        from utils import GetStockData

        # GetStockData 已改为延迟加载，这里主动触发一次，确保筛选页拿到全市场代码池。
        lazy_loader = getattr(GetStockData, "_load_result_dict_once", None)
        if callable(lazy_loader):
            try:
                lazy_loader()
            except Exception as e:
                logger.warning("触发 GetStockData 代码池延迟加载失败: %s", e)

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


def _fetch_gtimg_symbols_once(session: requests.Session, symbols: List[str], timeout: int = 25) -> Optional[List[dict]]:
    if not symbols:
        return []
    url = "https://qt.gtimg.cn/q=" + ",".join(symbols)
    r = session.get(url, headers=_GTIMG_HEADERS, timeout=timeout)
    r.raise_for_status()
    text = r.content.decode("gbk", errors="replace")
    return _parse_gtimg_response(text)


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
                all_rows.extend(_fetch_gtimg_symbols_once(session, batch, timeout=25) or [])
                break
            except Exception as e:
                last_err = e
                time.sleep(0.6 * (2**attempt) + 0.1)
        else:
            logger.warning("腾讯行情批次失败 symbols=%s.. err=%s", batch[:3], last_err)
            # 降级补拉：按小批次甚至单票兜底，避免整批缺失导致筛选漏票。
            for j in range(0, len(batch), 10):
                mini = batch[j : j + 10]
                mini_ok = False
                for mini_attempt in range(3):
                    try:
                        all_rows.extend(_fetch_gtimg_symbols_once(session, mini, timeout=15) or [])
                        mini_ok = True
                        break
                    except Exception:
                        time.sleep(0.35 * (mini_attempt + 1))
                if mini_ok:
                    continue
                for sym in mini:
                    single_ok = False
                    for single_attempt in range(2):
                        try:
                            all_rows.extend(_fetch_gtimg_symbols_once(session, [sym], timeout=12) or [])
                            single_ok = True
                            break
                        except Exception:
                            time.sleep(0.25 * (single_attempt + 1))
                    if not single_ok:
                        logger.warning("腾讯行情单票兜底失败 symbol=%s", sym)
        time.sleep(0.06)
    dedup: Dict[str, dict] = {}
    for item in all_rows:
        sym = str(item.get("gtimg_symbol") or "").strip().lower()
        if not sym:
            continue
        dedup[sym] = item
    return list(dedup.values())


def _parse_trade_date_input(trade_date: Optional[str]) -> Optional[date]:
    text = str(trade_date or "").strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("trade_date 格式需为 YYYY-MM-DD 或 YYYYMMDD")


def _resolve_effective_trade_date(requested_date: date) -> Optional[date]:
    rows = exeQuery(
        "SELECT MAX(trade_date) AS trade_date FROM stock_daily_kline WHERE trade_date <= %s",
        (requested_date,),
    ) or []
    raw = (rows[0] or {}).get("trade_date") if rows else None
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if hasattr(raw, "isoformat"):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def _load_daily_snapshot_map(trade_date: date) -> Dict[str, dict]:
    rows = exeQuery(
        """
        SELECT ts_code, close, pre_close, pct_chg, change_amount, turnover_rate, vol, amount
        FROM stock_daily_kline
        WHERE trade_date = %s
        """,
        (trade_date,),
    ) or []

    out: Dict[str, dict] = {}
    for item in rows:
        ts_code = str(item.get("ts_code") or "").strip().upper()
        if not ts_code:
            continue
        close = _safe_float(item.get("close"))
        pre_close = _safe_float(item.get("pre_close"))
        pct_chg = _safe_float(item.get("pct_chg"))
        change_amount = _safe_float(item.get("change_amount"))
        if pct_chg is None and close is not None and pre_close is not None and pre_close != 0:
            pct_chg = ((close - pre_close) / pre_close) * 100
        if change_amount is None and close is not None and pre_close is not None:
            change_amount = close - pre_close

        out[ts_code] = {
            "close": close,
            "pct_chg": pct_chg,
            "change_amount": change_amount,
            "turnover_rate": _safe_float(item.get("turnover_rate")),
            "vol": _safe_float(item.get("vol")),
            "amount": _safe_float(item.get("amount")),
            "pre_close": pre_close,
            "pct_recomputed": False,
        }

    if not out:
        return out

    ts_codes = list(out.keys())
    prev_close_map: Dict[str, float] = {}
    chunk_size = 600
    for i in range(0, len(ts_codes), chunk_size):
        chunk_codes = ts_codes[i : i + chunk_size]
        if not chunk_codes:
            continue
        placeholders = ", ".join(["%s"] * len(chunk_codes))
        query = f"""
            SELECT t.ts_code, t.close
            FROM stock_daily_kline t
            INNER JOIN (
                SELECT ts_code, MAX(trade_date) AS prev_trade_date
                FROM stock_daily_kline
                WHERE trade_date < %s
                  AND ts_code IN ({placeholders})
                GROUP BY ts_code
            ) p
              ON p.ts_code = t.ts_code
             AND p.prev_trade_date = t.trade_date
        """
        params = (trade_date, *chunk_codes)
        prev_rows = exeQuery(query, params) or []
        for prev in prev_rows:
            ts = str(prev.get("ts_code") or "").strip().upper()
            close_val = _safe_float(prev.get("close"))
            if ts and close_val is not None and close_val > 0:
                prev_close_map[ts] = close_val

    # 用前一交易日收盘价重算涨跌幅，避免历史数据中 pre_close 异常造成漏筛。
    for ts_code, snap in out.items():
        close = _safe_float(snap.get("close"))
        prev_close_ref = _safe_float(prev_close_map.get(ts_code))
        if close is None or prev_close_ref is None or prev_close_ref <= 0:
            continue
        change_amount = close - prev_close_ref
        pct_chg = (change_amount / prev_close_ref) * 100
        snap["pre_close"] = prev_close_ref
        snap["change_amount"] = change_amount
        snap["pct_chg"] = pct_chg
        snap["pct_recomputed"] = True
    return out


def _estimate_historical_mv_yi(current_mv_yi: Optional[float], current_price: Optional[float], historical_close: Optional[float]) -> Optional[float]:
    if current_mv_yi is None or current_price is None or historical_close is None:
        return None
    if current_price <= 0:
        return None
    return float(current_mv_yi) * float(historical_close) / float(current_price)


def screen_stocks_by_mv_and_pct(
    min_mv_yi: float,
    min_pct_chg: float,
    limit: int = 3000,
    trade_date: Optional[str] = None,
) -> Tuple[List[dict], dict]:
    requested_trade_date = _parse_trade_date_input(trade_date)
    effective_trade_date = None
    daily_snapshot_map: Dict[str, dict] = {}
    sym_to_meta: Dict[str, Tuple[str, str]] = {}
    symbols: List[str] = []

    if requested_trade_date is None:
        universe = _load_universe()
        if not universe:
            raise RuntimeError(
                "股票代码池为空（GetStockData/数据库stocks/东方财富接口均不可用）。"
                "请检查服务网络、数据库连接与启动日志。"
            )
        sym_to_meta = {u[0]: (u[1], u[2]) for u in universe}
        symbols = list(sym_to_meta.keys())
    else:
        # 历史模式：以日K表当日股票池为准，避免因代码池不全导致漏票。
        # 名称优先用全市场代码表补齐，失败时再用实时行情返回名。
        name_hint_by_ts: Dict[str, str] = {}
        try:
            for _, ts_code, stock_name in _load_universe():
                ts = str(ts_code or "").strip().upper()
                if not ts:
                    continue
                if ts not in name_hint_by_ts and stock_name:
                    name_hint_by_ts[ts] = str(stock_name).strip()
        except Exception as e:
            logger.warning("历史模式加载名称提示失败: %s", e)

        effective_trade_date = _resolve_effective_trade_date(requested_trade_date)
        if effective_trade_date is None:
            raise RuntimeError(
                "历史日期筛选失败：stock_daily_kline 无可用数据。"
                "请先执行 /api/daily_kline/sync_full 或 /api/daily_kline/sync_incremental。"
            )
        daily_snapshot_map = _load_daily_snapshot_map(effective_trade_date)
        if not daily_snapshot_map:
            raise RuntimeError(
                f"历史日期筛选失败：{effective_trade_date.isoformat()} 无日K数据。"
                "请先执行日K同步。"
            )
        for ts_code in daily_snapshot_map.keys():
            ts = str(ts_code or "").strip().upper()
            if not ts or _is_star_market_board(ts):
                continue
            sym = ts_to_gtimg_symbol(ts)
            if not sym:
                continue
            if sym in sym_to_meta:
                continue
            sym_to_meta[sym] = (ts, name_hint_by_ts.get(ts, ""))
        symbols = list(sym_to_meta.keys())
        if not symbols:
            raise RuntimeError(
                f"历史日期筛选失败：{effective_trade_date.isoformat()} 未找到可用股票代码。"
            )

    em_profile_map = _load_em_profile_map()

    tz_cn = timezone(timedelta(hours=8))
    fetched_at = datetime.now(tz_cn).isoformat(timespec="seconds")

    quotes = _fetch_gtimg_batches(symbols)
    if not quotes:
        raise RuntimeError("腾讯行情接口未返回有效数据，请检查网络或稍后重试。")

    rows: List[dict] = []
    row_ts_codes: List[str] = []
    historical_mv_estimated_count = 0
    historical_daily_matched_count = 0
    historical_pct_recomputed_count = 0
    matched_quote_symbols: set[str] = set()
    for q in quotes:
        gsym = q.get("gtimg_symbol")
        if not gsym or gsym not in sym_to_meta:
            continue
        matched_quote_symbols.add(str(gsym).lower())
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
        price = q.get("price")
        change_amount = q.get("change_amount")
        turnover_rate = q.get("turnover_rate")
        volume = None
        amount = None

        if effective_trade_date is not None:
            daily = daily_snapshot_map.get(ts_code.upper())
            if not daily:
                continue
            historical_daily_matched_count += 1
            if daily.get("pct_recomputed"):
                historical_pct_recomputed_count += 1
            price = daily.get("close")
            pct = daily.get("pct_chg")
            change_amount = daily.get("change_amount")
            turnover_rate = daily.get("turnover_rate")
            volume = daily.get("vol")
            amount = daily.get("amount")
            estimated_mv = _estimate_historical_mv_yi(
                current_mv_yi=q.get("total_mv_yi"),
                current_price=q.get("price"),
                historical_close=daily.get("close"),
            )
            if estimated_mv is not None:
                historical_mv_estimated_count += 1
            mv_yi = estimated_mv

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

        rows.append(
            {
                "stock_code": stock_code,
                "stock_name": nm,
                "price": price,
                "pct_chg": pct,
                "change_amount": change_amount,
                "total_mv_yi": round(mv_yi, 4),
                "float_mv_yi": float_mv_yi,
                "turnover_rate": round(turnover_rate, 4) if turnover_rate is not None else None,
                "board": board,
                "concept": concept,
                "future_events": [],
                "volume": volume,
                "amount": amount,
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

    ths_theme_trend_stats = _enrich_rows_with_ths_theme_trend(rows)

    meta: Dict[str, Any] = {
        "fetched_at": fetched_at,
        "source": "qt.gtimg.cn (Tencent, same vendor family as K-line fqkline)",
        "universe_size": len(symbols),
        "quotes_parsed": len(quotes),
        "quote_symbols_requested": len(symbols),
        "quote_symbols_matched": len(matched_quote_symbols),
        "quote_symbols_missing": max(0, len(symbols) - len(matched_quote_symbols)),
        "total_after_filter": len(rows),
        "em_profile_non_empty": em_profile_non_empty,
        "ths_fallback_filled": ths_fallback_filled,
        "ths_theme_trend_enabled": bool(ths_theme_trend_stats.get("enabled")),
        "ths_theme_board_matched": int(ths_theme_trend_stats.get("board_matched", 0) or 0),
        "ths_theme_concept_matched": int(ths_theme_trend_stats.get("concept_matched", 0) or 0),
        "ths_theme_fetched_at": ths_theme_trend_stats.get("fetched_at") or "",
        "ths_theme_source": ths_theme_trend_stats.get("source") or "",
        "exclude_star_board": True,
        "exclude_note": "已排除科创板（代码 688 开头）",
    }
    if effective_trade_date is None:
        meta["mode"] = "realtime"
    else:
        meta.update(
            {
                "mode": "historical",
                "requested_trade_date": requested_trade_date.isoformat() if requested_trade_date else "",
                "effective_trade_date": effective_trade_date.isoformat(),
                "daily_rows_available": len(daily_snapshot_map),
                "daily_rows_matched_quote": historical_daily_matched_count,
                "historical_mv_estimated_count": historical_mv_estimated_count,
                "historical_pct_recomputed_count": historical_pct_recomputed_count,
                "source": (
                    "stock_daily_kline + qt.gtimg.cn "
                    "(历史涨跌幅/收盘价 + 当前总股本估算历史市值)"
                ),
                "historical_note": (
                    "历史日期模式下，总市值按“当日收盘价 × 当前估算总股本”换算，"
                    "为近似值。"
                ),
                "ths_theme_note": "板块/概念走势优先同花顺，失败时回退东方财富实时榜单，与历史筛选日期不强绑定。",
            }
        )
    return rows, meta
