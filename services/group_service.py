"""
股票分组服务模块
提供分组的创建、查询、更新、删除等功能
"""
from datetime import datetime
import time
from threading import Lock
from copy import deepcopy
from config.dbconfig import db_pool


def get_connection():
    """获取数据库连接"""
    conn = db_pool.get_connection()
    if not conn.is_connected():
        conn.reconnect(attempts=3, delay=1)
    return conn


def _serialize_group_datetimes(group):
    """将分组 dict 中的 datetime 转为字符串（原地修改）"""
    for key, value in list(group.items()):
        if isinstance(value, datetime):
            group[key] = value.strftime('%Y-%m-%d %H:%M:%S')


# /api/groups 查询缓存（短 TTL，降低高频查询压力）
_GROUP_CACHE_LOCK = Lock()
_GROUP_CACHE = {
    True: {'expire_at': 0.0, 'data': None},   # include_stocks=True
    False: {'expire_at': 0.0, 'data': None},  # include_stocks=False
}
_GROUP_CACHE_TTL_SECONDS = {
    True: 60.0,
    False: 120.0,
}
# /api/groups/<id> 与 /api/groups/code/<code> 查询缓存（短 TTL）
_GROUP_DETAIL_CACHE_LOCK = Lock()
_GROUP_DETAIL_CACHE = {}
_GROUP_DETAIL_CACHE_TTL_SECONDS = {
    True: 30.0,
    False: 120.0,
}
_GROUP_INDEX_INIT_LOCK = Lock()
_GROUP_INDEX_INIT_DONE = False


def _get_cached_groups(include_stocks):
    now = time.time()
    with _GROUP_CACHE_LOCK:
        entry = _GROUP_CACHE.get(include_stocks)
        if not entry:
            return None
        if entry['data'] is None or entry['expire_at'] <= now:
            return None
        return deepcopy(entry['data'])


def _set_cached_groups(include_stocks, groups):
    ttl = _GROUP_CACHE_TTL_SECONDS.get(include_stocks, 10.0)
    with _GROUP_CACHE_LOCK:
        _GROUP_CACHE[include_stocks] = {
            'expire_at': time.time() + ttl,
            'data': deepcopy(groups),
        }


def _get_cached_group_detail(cache_key):
    now = time.time()
    with _GROUP_DETAIL_CACHE_LOCK:
        entry = _GROUP_DETAIL_CACHE.get(cache_key)
        if not entry:
            return None
        if entry['data'] is None or entry['expire_at'] <= now:
            _GROUP_DETAIL_CACHE.pop(cache_key, None)
            return None
        return deepcopy(entry['data'])


def _set_cached_group_detail(cache_key, include_stocks, group_detail):
    ttl = _GROUP_DETAIL_CACHE_TTL_SECONDS.get(include_stocks, 20.0)
    with _GROUP_DETAIL_CACHE_LOCK:
        _GROUP_DETAIL_CACHE[cache_key] = {
            'expire_at': time.time() + ttl,
            'data': deepcopy(group_detail),
        }


def _invalidate_group_cache():
    with _GROUP_CACHE_LOCK:
        _GROUP_CACHE[True] = {'expire_at': 0.0, 'data': None}
        _GROUP_CACHE[False] = {'expire_at': 0.0, 'data': None}
    with _GROUP_DETAIL_CACHE_LOCK:
        _GROUP_DETAIL_CACHE.clear()


def _ensure_group_indexes_once():
    """
    为 /api/groups 的核心查询补齐复合索引（仅进程内首次执行一次）：
    - stock_groups(is_active, sort_order, id)
    - stock_group_items(group_id, sort_order, id)
    """
    global _GROUP_INDEX_INIT_DONE
    if _GROUP_INDEX_INIT_DONE:
        return

    with _GROUP_INDEX_INIT_LOCK:
        if _GROUP_INDEX_INIT_DONE:
            return

        conn = None
        cursor = None
        try:
            conn = get_connection()
            # 使用 buffered cursor，避免 SHOW INDEX 结果未完全消费导致 Unread result found
            cursor = conn.cursor(dictionary=True, buffered=True)

            cursor.execute(
                "SHOW INDEX FROM stock_groups WHERE Key_name = %s",
                ('idx_groups_active_sort_id',),
            )
            if not cursor.fetchall():
                cursor.execute(
                    """
                    CREATE INDEX idx_groups_active_sort_id
                    ON stock_groups (is_active, sort_order, id)
                    """
                )

            cursor.execute(
                "SHOW INDEX FROM stock_group_items WHERE Key_name = %s",
                ('idx_items_group_sort_id',),
            )
            if not cursor.fetchall():
                cursor.execute(
                    """
                    CREATE INDEX idx_items_group_sort_id
                    ON stock_group_items (group_id, sort_order, id)
                    """
                )

            _GROUP_INDEX_INIT_DONE = True
        except Exception as e:
            # 索引创建失败不影响业务请求，继续走原逻辑
            print(f"分组索引检查/创建失败（可忽略）: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def get_all_groups(include_stocks=True):
    """
    获取所有分组

    含股票列表时使用「分组表 1 次 + 明细表 1 次」查询，避免按分组 N+1 次查询。
    """
    _ensure_group_indexes_once()
    cached = _get_cached_groups(include_stocks)
    if cached is not None:
        return cached

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        if include_stocks:
            cursor.execute(
                """
                SELECT g.*
                FROM stock_groups g
                WHERE g.is_active = 1
                ORDER BY g.sort_order, g.id
                """
            )
            groups = cursor.fetchall()
            if not groups:
                return []

            group_ids = [g['id'] for g in groups]
            placeholders = ','.join(['%s'] * len(group_ids))
            cursor.execute(
                f"""
                SELECT id, group_id, stock_code, stock_name, sort_order
                FROM stock_group_items
                WHERE group_id IN ({placeholders})
                ORDER BY group_id, sort_order, id
                """,
                tuple(group_ids),
            )
            items_by_group = {gid: [] for gid in group_ids}
            for row in cursor.fetchall():
                gid = row['group_id']
                if gid in items_by_group:
                    items_by_group[gid].append(
                        {
                            'id': row['id'],
                            'stock_code': row['stock_code'],
                            'stock_name': row['stock_name'],
                            'sort_order': row['sort_order'],
                        }
                    )

            for group in groups:
                _serialize_group_datetimes(group)
                stocks = items_by_group.get(group['id'], [])
                group['stocks'] = stocks
                group['stock_count'] = len(stocks)
        else:
            # 两段查询替代 LEFT JOIN + GROUP BY，避免在大明细表上全表聚合。
            cursor.execute(
                """
                SELECT g.*
                FROM stock_groups g
                WHERE g.is_active = 1
                ORDER BY g.sort_order, g.id
                """
            )
            groups = cursor.fetchall()
            if not groups:
                return []

            group_ids = [g['id'] for g in groups]
            placeholders = ','.join(['%s'] * len(group_ids))
            cursor.execute(
                f"""
                SELECT group_id, COUNT(*) AS stock_count
                FROM stock_group_items
                WHERE group_id IN ({placeholders})
                GROUP BY group_id
                """,
                tuple(group_ids),
            )
            count_map = {row['group_id']: int(row.get('stock_count') or 0) for row in cursor.fetchall()}

            for group in groups:
                _serialize_group_datetimes(group)
                group['stock_count'] = count_map.get(group['id'], 0)

        _set_cached_groups(include_stocks, groups)
        return groups

    except Exception as e:
        print(f"查询分组失败: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_group(group_id, include_stocks=True):
    """
    获取单个分组详情
    
    Args:
        group_id: 分组ID
        include_stocks: 是否返回股票明细（False 时仅返回 stock_count）
        
    Returns:
        dict: 分组详情
    """
    _ensure_group_indexes_once()
    cache_key = ('id', int(group_id), bool(include_stocks))
    cached = _get_cached_group_detail(cache_key)
    if cached is not None:
        return cached

    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM stock_groups WHERE id = %s", (group_id,))
        group = cursor.fetchone()
        
        if group:
            # 处理日期时间序列化
            _serialize_group_datetimes(group)

            if include_stocks:
                cursor.execute(
                    """SELECT id, stock_code, stock_name, sort_order
                       FROM stock_group_items
                       WHERE group_id = %s
                       ORDER BY sort_order, id""",
                    (group_id,)
                )
                stocks = cursor.fetchall()
                group['stocks'] = stocks
                group['stock_count'] = len(stocks)
            else:
                cursor.execute(
                    "SELECT COUNT(*) AS stock_count FROM stock_group_items WHERE group_id = %s",
                    (group_id,),
                )
                count_row = cursor.fetchone() or {}
                group['stock_count'] = int(count_row.get('stock_count') or 0)
            _set_cached_group_detail(cache_key, include_stocks, group)
            
        return group
        
    except Exception as e:
        print(f"查询分组详情失败: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_group_by_code(group_code, include_stocks=True):
    """
    根据分组代码获取分组
    
    Args:
        group_code: 分组代码
        include_stocks: 是否返回股票明细（False 时仅返回 stock_count）
        
    Returns:
        dict: 分组详情
    """
    _ensure_group_indexes_once()
    cache_key = ('code', str(group_code), bool(include_stocks))
    cached = _get_cached_group_detail(cache_key)
    if cached is not None:
        return cached

    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM stock_groups WHERE group_code = %s", (group_code,))
        group = cursor.fetchone()
        
        if group:
            _serialize_group_datetimes(group)

            if include_stocks:
                cursor.execute(
                    """SELECT id, stock_code, stock_name, sort_order
                       FROM stock_group_items
                       WHERE group_id = %s
                       ORDER BY sort_order, id""",
                    (group['id'],)
                )
                stocks = cursor.fetchall()
                group['stocks'] = stocks
                group['stock_count'] = len(stocks)
            else:
                cursor.execute(
                    "SELECT COUNT(*) AS stock_count FROM stock_group_items WHERE group_id = %s",
                    (group['id'],),
                )
                count_row = cursor.fetchone() or {}
                group['stock_count'] = int(count_row.get('stock_count') or 0)

            _set_cached_group_detail(cache_key, include_stocks, group)
            
        return group
        
    except Exception as e:
        print(f"查询分组详情失败: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_group(group_data):
    """
    创建分组
    
    Args:
        group_data: 分组数据字典
        
    Returns:
        dict: 包含 success, group_id 或 error message
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 检查分组代码是否已存在
        cursor.execute(
            "SELECT id FROM stock_groups WHERE group_code = %s",
            (group_data.get('groupCode'),)
        )
        if cursor.fetchone():
            return {'success': False, 'message': '分组代码已存在'}
        
        # 获取最大排序号
        cursor.execute("SELECT MAX(sort_order) as max_order FROM stock_groups")
        result = cursor.fetchone()
        max_order = result['max_order'] or 0
        
        sql = """
            INSERT INTO stock_groups (group_name, group_code, description, color, sort_order, is_default)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        params = (
            group_data.get('groupName'),
            group_data.get('groupCode'),
            group_data.get('description', ''),
            group_data.get('color', '#667eea'),
            group_data.get('sortOrder', max_order + 1),
            group_data.get('isDefault', 0)
        )
        
        cursor.execute(sql, params)
        group_id = cursor.lastrowid
        
        # 如果有股票列表，添加股票
        stocks = group_data.get('stocks', [])
        for idx, stock in enumerate(stocks):
            cursor.execute(
                """INSERT INTO stock_group_items (group_id, stock_code, stock_name, sort_order)
                   VALUES (%s, %s, %s, %s)""",
                (group_id, stock.get('stockCode'), stock.get('stockName', ''), idx + 1)
            )
        
        conn.commit()
        _invalidate_group_cache()
        
        return {
            'success': True,
            'groupId': group_id,
            'message': '分组创建成功'
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"创建分组失败: {e}")
        return {'success': False, 'message': f'创建分组失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_group(group_id, group_data):
    """
    更新分组
    
    Args:
        group_id: 分组ID
        group_data: 分组数据字典
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 检查分组是否存在
        cursor.execute("SELECT id FROM stock_groups WHERE id = %s", (group_id,))
        if not cursor.fetchone():
            return {'success': False, 'message': '分组不存在'}
        
        # 如果修改了分组代码，检查新代码是否已存在
        new_code = group_data.get('groupCode')
        if new_code:
            cursor.execute(
                "SELECT id FROM stock_groups WHERE group_code = %s AND id != %s",
                (new_code, group_id)
            )
            if cursor.fetchone():
                return {'success': False, 'message': '分组代码已存在'}
        
        # 构建更新SQL
        update_fields = []
        params = []
        
        if 'groupName' in group_data:
            update_fields.append("group_name = %s")
            params.append(group_data['groupName'])
        if 'groupCode' in group_data:
            update_fields.append("group_code = %s")
            params.append(group_data['groupCode'])
        if 'description' in group_data:
            update_fields.append("description = %s")
            params.append(group_data['description'])
        if 'color' in group_data:
            update_fields.append("color = %s")
            params.append(group_data['color'])
        if 'sortOrder' in group_data:
            update_fields.append("sort_order = %s")
            params.append(group_data['sortOrder'])
        if 'isDefault' in group_data:
            update_fields.append("is_default = %s")
            params.append(group_data['isDefault'])
        
        if update_fields:
            sql = f"UPDATE stock_groups SET {', '.join(update_fields)} WHERE id = %s"
            params.append(group_id)
            cursor.execute(sql, params)
        
        conn.commit()
        _invalidate_group_cache()
        
        return {'success': True, 'message': '分组更新成功'}
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"更新分组失败: {e}")
        return {'success': False, 'message': f'更新分组失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def delete_group(group_id):
    """
    删除分组
    
    Args:
        group_id: 分组ID
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 检查是否为默认分组
        cursor.execute("SELECT is_default FROM stock_groups WHERE id = %s", (group_id,))
        group = cursor.fetchone()
        
        if not group:
            return {'success': False, 'message': '分组不存在'}
        
        if group['is_default']:
            return {'success': False, 'message': '不能删除默认分组'}
        
        # 删除分组（关联的股票会通过外键级联删除）
        cursor.execute("DELETE FROM stock_groups WHERE id = %s", (group_id,))
        
        conn.commit()
        _invalidate_group_cache()
        
        return {'success': True, 'message': '分组删除成功'}
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"删除分组失败: {e}")
        return {'success': False, 'message': f'删除分组失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def add_stock_to_group(group_id, stock_code, stock_name=''):
    """
    添加股票到分组
    
    Args:
        group_id: 分组ID
        stock_code: 股票代码
        stock_name: 股票名称
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 检查分组是否存在
        cursor.execute("SELECT id FROM stock_groups WHERE id = %s", (group_id,))
        if not cursor.fetchone():
            return {'success': False, 'message': '分组不存在'}
        
        # 检查股票是否已在分组中
        cursor.execute(
            "SELECT id FROM stock_group_items WHERE group_id = %s AND stock_code = %s",
            (group_id, stock_code)
        )
        if cursor.fetchone():
            return {'success': False, 'message': '股票已在该分组中'}
        
        # 获取最大排序号
        cursor.execute(
            "SELECT MAX(sort_order) as max_order FROM stock_group_items WHERE group_id = %s",
            (group_id,)
        )
        result = cursor.fetchone()
        max_order = result['max_order'] or 0
        
        # 添加股票
        cursor.execute(
            """INSERT INTO stock_group_items (group_id, stock_code, stock_name, sort_order)
               VALUES (%s, %s, %s, %s)""",
            (group_id, stock_code, stock_name, max_order + 1)
        )
        
        item_id = cursor.lastrowid
        conn.commit()
        _invalidate_group_cache()
        
        return {
            'success': True,
            'itemId': item_id,
            'message': '股票添加成功'
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"添加股票失败: {e}")
        return {'success': False, 'message': f'添加股票失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def add_stocks_batch_to_group(group_id, stocks):
    """
    批量添加股票到分组（单事务）。已存在的代码跳过，不视为失败。

    Args:
        group_id: 分组 ID
        stocks: [{ 'stockCode'|'stock_code', 'stockName'|'stock_name' }, ...]

    Returns:
        dict: success, added, skipped, message
    """
    conn = None
    cursor = None

    if not stocks:
        return {'success': False, 'message': '股票列表为空'}

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT id FROM stock_groups WHERE id = %s", (group_id,))
        if not cursor.fetchone():
            return {'success': False, 'message': '分组不存在'}

        cursor.execute(
            "SELECT stock_code FROM stock_group_items WHERE group_id = %s",
            (group_id,),
        )
        existing = {row['stock_code'] for row in cursor.fetchall()}

        cursor.execute(
            "SELECT COALESCE(MAX(sort_order), 0) AS m FROM stock_group_items WHERE group_id = %s",
            (group_id,),
        )
        row_m = cursor.fetchone()
        max_order = int(row_m['m'] or 0)

        added = 0
        skipped = 0
        for item in stocks:
            if not isinstance(item, dict):
                skipped += 1
                continue
            code = (item.get('stockCode') or item.get('stock_code') or '').strip()
            if not code:
                skipped += 1
                continue
            if code in existing:
                skipped += 1
                continue
            name = item.get('stockName') or item.get('stock_name') or ''
            max_order += 1
            cursor.execute(
                """INSERT INTO stock_group_items (group_id, stock_code, stock_name, sort_order)
                   VALUES (%s, %s, %s, %s)""",
                (group_id, code, name, max_order),
            )
            existing.add(code)
            added += 1

        conn.commit()
        if added > 0:
            _invalidate_group_cache()
        return {
            'success': True,
            'added': added,
            'skipped': skipped,
            'message': f'新增 {added} 只，跳过 {skipped} 只（已在分组或代码无效）',
        }

    except Exception as e:
        if conn:
            conn.rollback()
        print(f'批量添加股票失败: {e}')
        return {'success': False, 'message': f'批量添加失败: {str(e)}'}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def remove_stock_from_group(group_id, stock_code):
    """
    从分组中移除股票
    
    Args:
        group_id: 分组ID
        stock_code: 股票代码
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            "DELETE FROM stock_group_items WHERE group_id = %s AND stock_code = %s",
            (group_id, stock_code)
        )
        
        if cursor.rowcount == 0:
            return {'success': False, 'message': '股票不在该分组中'}
        
        conn.commit()
        _invalidate_group_cache()
        
        return {'success': True, 'message': '股票移除成功'}
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"移除股票失败: {e}")
        return {'success': False, 'message': f'移除股票失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_group_stocks(group_id, stocks):
    """
    更新分组的股票列表（全量更新）
    
    Args:
        group_id: 分组ID
        stocks: 股票列表 [{'stockCode': 'xxx', 'stockName': 'xxx'}, ...]
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 检查分组是否存在
        cursor.execute("SELECT id FROM stock_groups WHERE id = %s", (group_id,))
        if not cursor.fetchone():
            return {'success': False, 'message': '分组不存在'}
        
        # 删除原有股票
        cursor.execute("DELETE FROM stock_group_items WHERE group_id = %s", (group_id,))
        
        # 添加新股票
        for idx, stock in enumerate(stocks):
            cursor.execute(
                """INSERT INTO stock_group_items (group_id, stock_code, stock_name, sort_order)
                   VALUES (%s, %s, %s, %s)""",
                (group_id, stock.get('stockCode'), stock.get('stockName', ''), idx + 1)
            )
        
        conn.commit()
        _invalidate_group_cache()
        
        return {
            'success': True,
            'message': f'分组股票更新成功，共 {len(stocks)} 只股票'
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"更新分组股票失败: {e}")
        return {'success': False, 'message': f'更新分组股票失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

