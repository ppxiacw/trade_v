import logging
import os
import time

import requests

from utils.env_loader import load_local_env
from utils.send_dingding import generate_stock_image_url, resolve_dingtalk_chart_types


load_local_env()

_logger = logging.getLogger(__name__)
_TOKEN_CACHE = {
    'access_token': '',
    'expires_at': 0.0,
}
_REQUEST_TIMEOUT = (
    float(os.getenv('WECOM_CONNECT_TIMEOUT_SECONDS', '3')),
    float(os.getenv('WECOM_READ_TIMEOUT_SECONDS', '10')),
)


def _request_without_env_proxy(method, url, **kwargs):
    session = requests.Session()
    session.trust_env = False
    try:
        return session.request(method, url, **kwargs)
    finally:
        session.close()


def _get_wecom_config():
    return {
        'corp_id': os.getenv('WECOM_CORP_ID', '').strip(),
        'agent_id': os.getenv('WECOM_AGENT_ID', '').strip(),
        'app_secret': os.getenv('WECOM_APP_SECRET', '').strip(),
        'to_user': (os.getenv('WECOM_TO_USER', '@all') or '@all').strip() or '@all',
        'webhook_url': os.getenv('WECOM_WEBHOOK_URL', '').strip(),
    }


def _get_access_token():
    now = time.time()
    if _TOKEN_CACHE['access_token'] and now < _TOKEN_CACHE['expires_at'] - 120:
        return _TOKEN_CACHE['access_token']

    cfg = _get_wecom_config()
    missing = [key for key in ('corp_id', 'app_secret') if not cfg[key]]
    if missing:
        raise RuntimeError(f"企业微信配置缺失: {', '.join(missing)}")

    try:
        response = _request_without_env_proxy(
            'GET',
            'https://qyapi.weixin.qq.com/cgi-bin/gettoken',
            params={
                'corpid': cfg['corp_id'],
                'corpsecret': cfg['app_secret'],
            },
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"获取企业微信 access_token 请求失败: {type(exc).__name__}") from exc
    payload = response.json()
    if payload.get('errcode') != 0:
        raise RuntimeError(f"获取企业微信 access_token 失败: {payload}")

    _TOKEN_CACHE['access_token'] = payload['access_token']
    _TOKEN_CACHE['expires_at'] = now + int(payload.get('expires_in') or 7200)
    return _TOKEN_CACHE['access_token']


def _build_markdown_message(title, ts_code, chart_period=None):
    (primary_k, primary_label), (secondary_k, secondary_label) = resolve_dingtalk_chart_types(chart_period)

    lines = [
        f"### 股票告警",
        f"> {title}",
    ]
    try:
        primary_url = generate_stock_image_url(ts_code, primary_k)
        secondary_url = generate_stock_image_url(ts_code, secondary_k)
        lines.extend([
            "",
            f"[查看{primary_label}走势图]({primary_url})",
            f"[查看{secondary_label}走势图]({secondary_url})",
        ])
    except Exception as exc:
        _logger.debug("企业微信告警附图链接生成失败: %s", exc)

    return "\n".join(lines)


def _build_text_message(title, ts_code, chart_period=None):
    (primary_k, primary_label), (secondary_k, secondary_label) = resolve_dingtalk_chart_types(chart_period)
    lines = [
        "股票告警",
        title,
    ]
    try:
        primary_url = generate_stock_image_url(ts_code, primary_k)
        secondary_url = generate_stock_image_url(ts_code, secondary_k)
        lines.extend([
            "",
            f"{primary_label}走势图: {primary_url}",
            f"{secondary_label}走势图: {secondary_url}",
        ])
    except Exception as exc:
        _logger.debug("企业微信告警附图链接生成失败: %s", exc)
    return "\n".join(lines)


def send_wecom_message(title, ts_code, chart_period=None):
    cfg = _get_wecom_config()
    if not cfg['agent_id']:
        raise RuntimeError('企业微信配置缺失: agent_id')

    content = _build_text_message(title, ts_code, chart_period)
    data = {
        'touser': cfg['to_user'],
        'msgtype': 'text',
        'agentid': int(cfg['agent_id']),
        'text': {
            'content': content,
        },
        'safe': 0,
        'enable_duplicate_check': 0,
    }

    _logger.info("%s\n", title)
    if os.getenv('ENABLE_REQUESTS') is not None:
        return

    try:
        access_token = _get_access_token()
        response = _request_without_env_proxy(
            'POST',
            'https://qyapi.weixin.qq.com/cgi-bin/message/send',
            params={'access_token': access_token},
            json=data,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get('errcode') != 0:
            _logger.warning("企业微信推送失败: %s", payload)
    except Exception as exc:
        _logger.warning("企业微信推送异常: %s", exc)


def send_wecom_webhook_message(title, ts_code, chart_period=None):
    cfg = _get_wecom_config()
    if not cfg['webhook_url']:
        raise RuntimeError('企业微信群机器人配置缺失: webhook_url')

    content = _build_text_message(title, ts_code, chart_period)
    data = {
        'msgtype': 'text',
        'text': {
            'content': content,
        },
    }

    _logger.info("%s\n", title)
    if os.getenv('ENABLE_REQUESTS') is not None:
        return

    try:
        response = _request_without_env_proxy(
            'POST',
            cfg['webhook_url'],
            json=data,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get('errcode') != 0:
            _logger.warning("企业微信群机器人推送失败: %s", payload)
    except Exception as exc:
        _logger.warning("企业微信群机器人推送异常: %s", type(exc).__name__)
