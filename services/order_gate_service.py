"""
下单条件判定：信号与组合规则（MySQL 持久化）
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from config.dbconfig import db_pool

DEFAULT_SIGNALS = [
    {'id': 'm30', 'label': '30分钟满足', 'group': '周期', 'sortOrder': 1, 'enabled': True},
    {'id': 'm5', 'label': '5分钟满足', 'group': '周期', 'sortOrder': 2, 'enabled': True},
    {'id': 'm1', 'label': '1分钟满足', 'group': '周期', 'sortOrder': 3, 'enabled': True},
    {'id': 'keyLevel', 'label': '关键点位满足', 'group': '结构', 'sortOrder': 4, 'enabled': True},
    {'id': 'm1_accel', 'label': '1分钟加速', 'group': '1分钟', 'sortOrder': 5, 'enabled': True},
    {'id': 'm1_div', 'label': '1分钟背离', 'group': '1分钟', 'sortOrder': 6, 'enabled': True},
    {'id': 'm5_accel', 'label': '5分钟加速', 'group': '5分钟', 'sortOrder': 7, 'enabled': True},
    {'id': 'm5_div', 'label': '5分钟背离', 'group': '5分钟', 'sortOrder': 8, 'enabled': True},
]

DEFAULT_COMBOS = [
    {
        'id': 'combo_1',
        'name': '组合一',
        'weight': 50,
        'enabled': True,
        'sortOrder': 1,
        'clauses': [
            {'id': 'c1a', 'type': 'all', 'signalIds': ['m30']},
            {'id': 'c1b', 'type': 'any', 'signalIds': ['m1', 'm5']},
        ],
    },
    {
        'id': 'combo_2',
        'name': '组合二',
        'weight': 40,
        'enabled': True,
        'sortOrder': 2,
        'clauses': [
            {'id': 'c2a', 'type': 'all', 'signalIds': ['keyLevel']},
            {'id': 'c2b', 'type': 'any', 'signalIds': ['m1', 'm5']},
        ],
    },
    {
        'id': 'combo_3',
        'name': '组合三',
        'weight': 35,
        'enabled': True,
        'sortOrder': 3,
        'clauses': [
            {'id': 'c3a', 'type': 'all', 'signalIds': ['m30']},
            {'id': 'c3b', 'type': 'any', 'signalIds': ['m1_accel', 'm1_div', 'm5_accel', 'm5_div']},
        ],
    },
]


def get_connection():
    conn = db_pool.get_connection()
    if not conn.is_connected():
        conn.reconnect(attempts=3, delay=1)
    return conn


def _serialize_datetimes(item: dict):
    for key, value in list(item.items()):
        if isinstance(value, datetime):
            item[key] = value.strftime('%Y-%m-%d %H:%M:%S')


def _ensure_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS order_gate_signals (
            signal_id VARCHAR(64) PRIMARY KEY COMMENT '信号ID',
            label VARCHAR(128) NOT NULL COMMENT '显示名称',
            group_name VARCHAR(64) NOT NULL DEFAULT '其它' COMMENT '分组',
            sort_order INT NOT NULL DEFAULT 0 COMMENT '排序',
            is_enabled TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_signal_sort (sort_order, signal_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下单条件信号'
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS order_gate_combos (
            combo_id VARCHAR(64) PRIMARY KEY COMMENT '组合ID',
            name VARCHAR(128) NOT NULL COMMENT '组合名称',
            weight INT NOT NULL DEFAULT 0 COMMENT '权重百分比0-100',
            is_enabled TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用',
            sort_order INT NOT NULL DEFAULT 0 COMMENT '排序',
            clauses_json JSON NOT NULL COMMENT '子条件JSON',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_combo_sort (sort_order, combo_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下单条件组合'
        """
    )


def _row_to_signal(row: dict) -> dict:
    return {
        'id': row.get('signal_id'),
        'label': row.get('label') or row.get('signal_id'),
        'group': row.get('group_name') or '其它',
        'sortOrder': int(row.get('sort_order') or 0),
        'enabled': bool(int(row.get('is_enabled') or 0)),
    }


def _row_to_combo(row: dict) -> dict:
    clauses_raw = row.get('clauses_json')
    if isinstance(clauses_raw, str):
        try:
            clauses = json.loads(clauses_raw)
        except Exception:
            clauses = []
    elif isinstance(clauses_raw, (list, dict)):
        clauses = clauses_raw if isinstance(clauses_raw, list) else []
    else:
        clauses = []
    return {
        'id': row.get('combo_id'),
        'name': row.get('name') or row.get('combo_id'),
        'weight': int(row.get('weight') or 0),
        'enabled': bool(int(row.get('is_enabled') or 0)),
        'sortOrder': int(row.get('sort_order') or 0),
        'clauses': clauses if isinstance(clauses, list) else [],
    }


def _normalize_signal(item: dict) -> Optional[dict]:
    signal_id = str(item.get('id') or item.get('signalId') or '').strip()
    label = str(item.get('label') or '').strip()
    if not signal_id or not label:
        return None
    return {
        'id': signal_id[:64],
        'label': label[:128],
        'group': str(item.get('group') or '其它').strip()[:64] or '其它',
        'sortOrder': int(item.get('sortOrder') or item.get('sort_order') or 0),
        'enabled': bool(item.get('enabled', item.get('is_enabled', True))),
    }


def _normalize_combo(item: dict) -> Optional[dict]:
    combo_id = str(item.get('id') or item.get('comboId') or '').strip()
    name = str(item.get('name') or '').strip()
    if not combo_id or not name:
        return None
    clauses = item.get('clauses') or []
    if not isinstance(clauses, list):
        clauses = []
    normalized_clauses = []
    for clause in clauses:
        if not isinstance(clause, dict):
            continue
        clause_type = str(clause.get('type') or 'any').strip().lower()
        if clause_type not in ('all', 'any'):
            clause_type = 'any'
        signal_ids = clause.get('signalIds') or clause.get('signal_ids') or []
        if not isinstance(signal_ids, list):
            signal_ids = []
        normalized_clauses.append({
            'id': str(clause.get('id') or f"c_{len(normalized_clauses)}")[:64],
            'type': clause_type,
            'signalIds': [str(x).strip() for x in signal_ids if str(x).strip()],
        })
    weight = max(0, min(100, int(item.get('weight') or 0)))
    return {
        'id': combo_id[:64],
        'name': name[:128],
        'weight': weight,
        'enabled': bool(item.get('enabled', item.get('is_enabled', True))),
        'sortOrder': int(item.get('sortOrder') or item.get('sort_order') or 0),
        'clauses': normalized_clauses,
    }


def _fetch_config(cursor) -> Tuple[List[dict], List[dict]]:
    cursor.execute(
        """
        SELECT signal_id, label, group_name, sort_order, is_enabled
        FROM order_gate_signals
        ORDER BY sort_order ASC, signal_id ASC
        """
    )
    signals = [_row_to_signal(row) for row in (cursor.fetchall() or [])]

    cursor.execute(
        """
        SELECT combo_id, name, weight, is_enabled, sort_order, clauses_json
        FROM order_gate_combos
        ORDER BY sort_order ASC, combo_id ASC
        """
    )
    combos = [_row_to_combo(row) for row in (cursor.fetchall() or [])]
    return signals, combos


def _insert_defaults(cursor):
    for item in DEFAULT_SIGNALS:
        cursor.execute(
            """
            INSERT INTO order_gate_signals (signal_id, label, group_name, sort_order, is_enabled)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                label = VALUES(label),
                group_name = VALUES(group_name),
                sort_order = VALUES(sort_order),
                is_enabled = VALUES(is_enabled)
            """,
            (
                item['id'],
                item['label'],
                item['group'],
                int(item['sortOrder']),
                1 if item['enabled'] else 0,
            ),
        )
    for item in DEFAULT_COMBOS:
        cursor.execute(
            """
            INSERT INTO order_gate_combos (combo_id, name, weight, is_enabled, sort_order, clauses_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                weight = VALUES(weight),
                is_enabled = VALUES(is_enabled),
                sort_order = VALUES(sort_order),
                clauses_json = VALUES(clauses_json)
            """,
            (
                item['id'],
                item['name'],
                int(item['weight']),
                1 if item['enabled'] else 0,
                int(item['sortOrder']),
                json.dumps(item['clauses'], ensure_ascii=False),
            ),
        )


def get_config(seed_if_empty: bool = True) -> Dict[str, Any]:
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_tables(cursor)

        signals, combos = _fetch_config(cursor)
        if seed_if_empty and (not signals or not combos):
            _insert_defaults(cursor)
            conn.commit()
            signals, combos = _fetch_config(cursor)

        return {'success': True, 'signals': signals, 'combos': combos}
    except Exception as e:
        return {'success': False, 'message': str(e), 'signals': [], 'combos': []}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def save_config(signals: List[dict], combos: List[dict]) -> Dict[str, Any]:
    normalized_signals = []
    seen_signal_ids = set()
    for raw in signals or []:
        item = _normalize_signal(raw)
        if not item or item['id'] in seen_signal_ids:
            continue
        seen_signal_ids.add(item['id'])
        normalized_signals.append(item)

    normalized_combos = []
    seen_combo_ids = set()
    for raw in combos or []:
        item = _normalize_combo(raw)
        if not item or item['id'] in seen_combo_ids:
            continue
        seen_combo_ids.add(item['id'])
        normalized_combos.append(item)

    if not normalized_signals:
        return {'success': False, 'message': '至少保留一个信号'}
    if not normalized_combos:
        return {'success': False, 'message': '至少保留一个组合'}

    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_tables(cursor)

        cursor.execute('DELETE FROM order_gate_signals')
        cursor.execute('DELETE FROM order_gate_combos')

        for item in normalized_signals:
            cursor.execute(
                """
                INSERT INTO order_gate_signals (signal_id, label, group_name, sort_order, is_enabled)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    item['id'],
                    item['label'],
                    item['group'],
                    int(item['sortOrder']),
                    1 if item['enabled'] else 0,
                ),
            )

        for item in normalized_combos:
            cursor.execute(
                """
                INSERT INTO order_gate_combos (combo_id, name, weight, is_enabled, sort_order, clauses_json)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    item['id'],
                    item['name'],
                    int(item['weight']),
                    1 if item['enabled'] else 0,
                    int(item['sortOrder']),
                    json.dumps(item['clauses'], ensure_ascii=False),
                ),
            )

        conn.commit()
        return {
            'success': True,
            'message': '配置已保存',
            'signals': normalized_signals,
            'combos': normalized_combos,
        }
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def reset_to_defaults() -> Dict[str, Any]:
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_tables(cursor)
        cursor.execute('DELETE FROM order_gate_signals')
        cursor.execute('DELETE FROM order_gate_combos')
        _insert_defaults(cursor)
        conn.commit()
        signals, combos = _fetch_config(cursor)
        return {
            'success': True,
            'message': '已恢复默认配置',
            'signals': signals,
            'combos': combos,
        }
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
