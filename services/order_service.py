"""
订单服务模块
提供订单的创建、查询、更新等功能
"""
import json
from datetime import datetime
from config.dbconfig import db_pool


def generate_order_no():
    """生成订单编号"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:17]
    return f'ORD{timestamp}'


def get_connection():
    """获取数据库连接"""
    conn = db_pool.get_connection()
    if not conn.is_connected():
        conn.reconnect(attempts=3, delay=1)
    return conn


def create_order(order_data):
    """
    创建订单
    
    Args:
        order_data: 订单数据字典
        
    Returns:
        dict: 包含 success, order_id, order_no 或 error message
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        order_no = generate_order_no()
        
        # 处理下单理由
        order_reason = order_data.get('orderReason')
        order_reason_value = order_reason.get('value') if isinstance(order_reason, dict) else order_reason
        order_reason_label = order_reason.get('label') if isinstance(order_reason, dict) else ''
        
        # 构建插入SQL
        sql = """
            INSERT INTO orders (
                order_no, stock_code, stock_name,
                buy_price, current_price, take_profit_price, stop_loss_price,
                plan_amount, buy_shares, actual_amount, total_capital,
                expected_profit, expected_loss, profit_loss_ratio,
                order_reason, order_reason_label, take_profit_reason, stop_loss_reason,
                conditions_json, recommended_position, total_score, score_rate,
                status, created_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                'pending', NOW()
            )
        """
        
        # 准备参数
        conditions = order_data.get('conditions', {})
        conditions_json = json.dumps(conditions) if conditions else None
        
        params = (
            order_no,
            order_data.get('stockCode'),
            order_data.get('stockName'),
            order_data.get('buyPrice'),
            order_data.get('currentPrice'),
            order_data.get('takeProfitPoint'),
            order_data.get('stopLossPoint'),
            order_data.get('planAmount'),
            order_data.get('buyShares'),
            order_data.get('actualBuyAmount'),
            order_data.get('totalCapital'),
            order_data.get('expectedProfit'),
            order_data.get('expectedLoss'),
            order_data.get('profitLossRatio'),
            order_reason_value,
            order_reason_label,
            order_data.get('takeProfitReason'),
            order_data.get('stopLossReason'),
            conditions_json,
            order_data.get('recommendedPosition'),
            order_data.get('totalScore'),
            order_data.get('scoreRate'),
        )
        
        cursor.execute(sql, params)
        order_id = cursor.lastrowid
        
        # 记录状态日志
        log_sql = """
            INSERT INTO order_status_logs (order_id, order_no, old_status, new_status, note)
            VALUES (%s, %s, NULL, 'pending', '订单创建')
        """
        cursor.execute(log_sql, (order_id, order_no))
        
        conn.commit()
        
        return {
            'success': True,
            'orderId': order_id,
            'orderNo': order_no,
            'message': '订单创建成功'
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"创建订单失败: {e}")
        return {
            'success': False,
            'message': f'创建订单失败: {str(e)}'
        }
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_orders(status=None, stock_code=None, limit=50):
    """
    查询订单列表
    
    Args:
        status: 订单状态筛选
        stock_code: 股票代码筛选
        limit: 返回数量限制
        
    Returns:
        list: 订单列表
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        sql = "SELECT * FROM orders WHERE 1=1"
        params = []
        
        if status:
            sql += " AND status = %s"
            params.append(status)
            
        if stock_code:
            sql += " AND stock_code = %s"
            params.append(stock_code)
            
        sql += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(sql, params)
        orders = cursor.fetchall()
        
        # 处理日期时间序列化
        for order in orders:
            for key, value in order.items():
                if isinstance(value, datetime):
                    order[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return orders
        
    except Exception as e:
        print(f"查询订单失败: {e}")
        return []
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_order(order_id):
    """
    查询单个订单详情
    
    Args:
        order_id: 订单ID
        
    Returns:
        dict: 订单详情
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
        order = cursor.fetchone()
        
        if order:
            # 处理日期时间序列化
            for key, value in order.items():
                if isinstance(value, datetime):
                    order[key] = value.strftime('%Y-%m-%d %H:%M:%S')
                    
            # 获取状态变更日志
            cursor.execute(
                "SELECT * FROM order_status_logs WHERE order_id = %s ORDER BY created_at",
                (order_id,)
            )
            logs = cursor.fetchall()
            for log in logs:
                for key, value in log.items():
                    if isinstance(value, datetime):
                        log[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            order['statusLogs'] = logs
            
        return order
        
    except Exception as e:
        print(f"查询订单详情失败: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_order_status(order_id, new_status, note=None):
    """
    更新订单状态
    
    Args:
        order_id: 订单ID
        new_status: 新状态
        note: 变更说明
        
    Returns:
        dict: 操作结果
    """
    conn = None
    cursor = None
    
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 获取当前状态
        cursor.execute("SELECT order_no, status FROM orders WHERE id = %s", (order_id,))
        order = cursor.fetchone()
        
        if not order:
            return {'success': False, 'message': '订单不存在'}
            
        old_status = order['status']
        order_no = order['order_no']
        
        # 更新状态
        update_sql = "UPDATE orders SET status = %s"
        params = [new_status]
        
        if new_status == 'executed':
            update_sql += ", executed_at = NOW()"
        elif new_status == 'closed':
            update_sql += ", closed_at = NOW()"
            
        update_sql += " WHERE id = %s"
        params.append(order_id)
        
        cursor.execute(update_sql, params)
        
        # 记录日志
        log_sql = """
            INSERT INTO order_status_logs (order_id, order_no, old_status, new_status, note)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(log_sql, (order_id, order_no, old_status, new_status, note))
        
        conn.commit()
        
        return {
            'success': True,
            'message': f'订单状态已更新为 {new_status}'
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"更新订单状态失败: {e}")
        return {'success': False, 'message': f'更新失败: {str(e)}'}
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def cancel_order(order_id, reason=None):
    """
    取消订单
    
    Args:
        order_id: 订单ID
        reason: 取消原因
        
    Returns:
        dict: 操作结果
    """
    return update_order_status(order_id, 'cancelled', reason or '用户取消')

