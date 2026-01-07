"""
股票分组服务模块
提供分组的创建、查询、更新、删除等功能
"""
from datetime import datetime
from config.dbconfig import db_pool


def get_connection():
    """获取数据库连接"""
    conn = db_pool.get_connection()
    if not conn.is_connected():
        conn.reconnect(attempts=3, delay=1)
    return conn


def get_all_groups(include_stocks=True):
    """
    获取所有分组
    
    Args:
        include_stocks: 是否包含分组下的股票列表
        
    Returns:
        list: 分组列表
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 查询所有分组
        sql = """
            SELECT g.*, COUNT(i.id) as stock_count 
            FROM stock_groups g 
            LEFT JOIN stock_group_items i ON g.id = i.group_id 
            WHERE g.is_active = 1 
            GROUP BY g.id 
            ORDER BY g.sort_order, g.id
        """
        cursor.execute(sql)
        groups = cursor.fetchall()
        
        # 处理日期时间序列化
        for group in groups:
            for key, value in group.items():
                if isinstance(value, datetime):
                    group[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            
            # 如果需要包含股票列表
            if include_stocks:
                cursor.execute(
                    """SELECT id, stock_code, stock_name, sort_order 
                       FROM stock_group_items 
                       WHERE group_id = %s 
                       ORDER BY sort_order, id""",
                    (group['id'],)
                )
                group['stocks'] = cursor.fetchall()
        
        return groups
        
    except Exception as e:
        print(f"查询分组失败: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_group(group_id):
    """
    获取单个分组详情
    
    Args:
        group_id: 分组ID
        
    Returns:
        dict: 分组详情
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM stock_groups WHERE id = %s", (group_id,))
        group = cursor.fetchone()
        
        if group:
            # 处理日期时间序列化
            for key, value in group.items():
                if isinstance(value, datetime):
                    group[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取股票列表
            cursor.execute(
                """SELECT id, stock_code, stock_name, sort_order 
                   FROM stock_group_items 
                   WHERE group_id = %s 
                   ORDER BY sort_order, id""",
                (group_id,)
            )
            group['stocks'] = cursor.fetchall()
            
        return group
        
    except Exception as e:
        print(f"查询分组详情失败: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_group_by_code(group_code):
    """
    根据分组代码获取分组
    
    Args:
        group_code: 分组代码
        
    Returns:
        dict: 分组详情
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM stock_groups WHERE group_code = %s", (group_code,))
        group = cursor.fetchone()
        
        if group:
            for key, value in group.items():
                if isinstance(value, datetime):
                    group[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute(
                """SELECT id, stock_code, stock_name, sort_order 
                   FROM stock_group_items 
                   WHERE group_id = %s 
                   ORDER BY sort_order, id""",
                (group['id'],)
            )
            group['stocks'] = cursor.fetchall()
            
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

