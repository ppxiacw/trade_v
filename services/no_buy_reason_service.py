"""
不可买原因服务
提供不可买原因的查询与维护能力（MySQL 持久化）。
"""
from datetime import datetime
from config.dbconfig import db_pool


def get_connection():
    conn = db_pool.get_connection()
    if not conn.is_connected():
        conn.reconnect(attempts=3, delay=1)
    return conn


def _serialize_datetimes(item):
    for key, value in list(item.items()):
        if isinstance(value, datetime):
            item[key] = value.strftime('%Y-%m-%d %H:%M:%S')


def _ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS no_buy_reasons (
            id INT PRIMARY KEY AUTO_INCREMENT,
            reason_code VARCHAR(64) NOT NULL COMMENT '原因唯一编码',
            reason_name VARCHAR(128) NOT NULL COMMENT '原因名称',
            description VARCHAR(255) DEFAULT '' COMMENT '说明',
            sort_order INT NOT NULL DEFAULT 0 COMMENT '排序',
            is_active TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_reason_code (reason_code),
            KEY idx_reason_active_sort (is_active, sort_order, id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='不可买原因维护表'
        """
    )


def _ensure_delete_log_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS no_buy_reason_delete_logs (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            stock_code VARCHAR(20) NOT NULL COMMENT '被过滤股票代码',
            stock_name VARCHAR(64) DEFAULT '' COMMENT '被过滤股票名称',
            reason_code VARCHAR(64) DEFAULT '' COMMENT '不可买原因编码',
            reason_name VARCHAR(128) NOT NULL COMMENT '不可买原因名称',
            source_page VARCHAR(64) DEFAULT 'kline' COMMENT '来源页面',
            context_key VARCHAR(128) DEFAULT '' COMMENT '上下文标识',
            note VARCHAR(255) DEFAULT '' COMMENT '备注',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            KEY idx_delete_log_created_at (created_at),
            KEY idx_delete_log_stock_code (stock_code),
            KEY idx_delete_log_reason_code (reason_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='不可买便签过滤记录'
        """
    )


def _normalize_reason_code(raw):
    text = str(raw or '').strip().lower()
    if not text:
        return ''
    out = []
    for ch in text:
        if ch.isalnum() or ch in ('_', '-'):
            out.append(ch)
        elif ch in (' ', '.', '/'):
            out.append('_')
    normalized = ''.join(out).strip('_')
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    return normalized[:64]


def list_reasons(active_only=False):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)

        sql = """
            SELECT id, reason_code, reason_name, description, sort_order, is_active, created_at, updated_at
            FROM no_buy_reasons
        """
        params = []
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY sort_order ASC, id ASC"
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall() or []
        for row in rows:
            _serialize_datetimes(row)
            row['is_active'] = int(row.get('is_active') or 0)
        return rows
    except Exception:
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_reason(data):
    conn = None
    cursor = None
    try:
        reason_name = str((data or {}).get('reasonName') or '').strip()
        if not reason_name:
            return {'success': False, 'message': 'reasonName 不能为空'}

        reason_code_raw = (data or {}).get('reasonCode') or reason_name
        reason_code = _normalize_reason_code(reason_code_raw)
        if not reason_code:
            return {'success': False, 'message': 'reasonCode 不合法'}

        description = str((data or {}).get('description') or '').strip()
        sort_order = int((data or {}).get('sortOrder') or 0)
        is_active = 1 if int((data or {}).get('isActive', 1) or 0) else 0

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)

        cursor.execute("SELECT id FROM no_buy_reasons WHERE reason_code = %s", (reason_code,))
        if cursor.fetchone():
            return {'success': False, 'message': 'reasonCode 已存在'}

        cursor.execute(
            """
            INSERT INTO no_buy_reasons (reason_code, reason_name, description, sort_order, is_active)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (reason_code, reason_name, description, sort_order, is_active),
        )
        reason_id = cursor.lastrowid
        conn.commit()
        return {'success': True, 'reasonId': reason_id, 'message': '创建成功'}
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': f'创建失败: {str(e)}'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_reason(reason_id, data):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)

        cursor.execute("SELECT id FROM no_buy_reasons WHERE id = %s", (reason_id,))
        if not cursor.fetchone():
            return {'success': False, 'message': '记录不存在'}

        update_fields = []
        params = []

        if 'reasonName' in (data or {}):
            reason_name = str((data or {}).get('reasonName') or '').strip()
            if not reason_name:
                return {'success': False, 'message': 'reasonName 不能为空'}
            update_fields.append("reason_name = %s")
            params.append(reason_name)

        if 'reasonCode' in (data or {}):
            reason_code = _normalize_reason_code((data or {}).get('reasonCode'))
            if not reason_code:
                return {'success': False, 'message': 'reasonCode 不合法'}
            cursor.execute(
                "SELECT id FROM no_buy_reasons WHERE reason_code = %s AND id != %s",
                (reason_code, reason_id),
            )
            if cursor.fetchone():
                return {'success': False, 'message': 'reasonCode 已存在'}
            update_fields.append("reason_code = %s")
            params.append(reason_code)

        if 'description' in (data or {}):
            update_fields.append("description = %s")
            params.append(str((data or {}).get('description') or '').strip())

        if 'sortOrder' in (data or {}):
            update_fields.append("sort_order = %s")
            params.append(int((data or {}).get('sortOrder') or 0))

        if 'isActive' in (data or {}):
            update_fields.append("is_active = %s")
            params.append(1 if int((data or {}).get('isActive') or 0) else 0)

        if not update_fields:
            return {'success': True, 'message': '无变更'}

        sql = f"UPDATE no_buy_reasons SET {', '.join(update_fields)} WHERE id = %s"
        params.append(reason_id)
        cursor.execute(sql, tuple(params))
        conn.commit()
        return {'success': True, 'message': '更新成功'}
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': f'更新失败: {str(e)}'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def delete_reason(reason_id):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)

        cursor.execute("DELETE FROM no_buy_reasons WHERE id = %s", (reason_id,))
        if cursor.rowcount <= 0:
            return {'success': False, 'message': '记录不存在'}
        conn.commit()
        return {'success': True, 'message': '删除成功'}
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': f'删除失败: {str(e)}'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_delete_log(data):
    conn = None
    cursor = None
    try:
        payload = data or {}
        stock_code = str(payload.get('stockCode') or '').strip()
        reason_name = str(payload.get('reasonName') or '').strip()
        if not stock_code:
            return {'success': False, 'message': 'stockCode 不能为空'}
        if not reason_name:
            return {'success': False, 'message': 'reasonName 不能为空'}

        stock_name = str(payload.get('stockName') or '').strip()
        reason_code = _normalize_reason_code(payload.get('reasonCode'))
        source_page = str(payload.get('sourcePage') or 'kline').strip()[:64]
        context_key = str(payload.get('contextKey') or '').strip()[:128]
        note = str(payload.get('note') or '').strip()[:255]

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_table(cursor)
        _ensure_delete_log_table(cursor)

        cursor.execute(
            """
            INSERT INTO no_buy_reason_delete_logs (
                stock_code, stock_name, reason_code, reason_name, source_page, context_key, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (stock_code, stock_name, reason_code, reason_name, source_page, context_key, note),
        )
        log_id = cursor.lastrowid
        conn.commit()
        return {'success': True, 'logId': log_id, 'message': '记录成功'}
    except Exception as e:
        if conn:
            conn.rollback()
        return {'success': False, 'message': f'记录失败: {str(e)}'}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

