"""
订单服务模块
提供订单的创建、查询、更新等功能
"""
import csv
import hashlib
import io
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


def _normalize_stock_code(code):
    raw = str(code or '').strip().lower()
    if not raw:
        return ''
    if raw.startswith('sh') or raw.startswith('sz'):
        return raw
    if raw.isdigit() and len(raw) == 6:
        if raw.startswith('6'):
            return f'sh{raw}'
        if raw.startswith('0') or raw.startswith('3'):
            return f'sz{raw}'
    return raw


def _parse_trade_datetime(date_str, time_str):
    date_text = str(date_str or '').strip()
    time_text = str(time_str or '').strip()
    if not date_text:
        return None
    if len(date_text) == 8 and date_text.isdigit():
        date_text = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"
    if not time_text:
        time_text = '00:00:00'
    if len(time_text) == 5:
        time_text = f"{time_text}:00"
    try:
        parsed = datetime.strptime(f'{date_text} {time_text}', '%Y-%m-%d %H:%M:%S')
        return parsed.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


def _parse_float(value):
    text = str(value or '').replace(',', '').strip()
    if text == '':
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_int(value):
    text = str(value or '').replace(',', '').strip()
    if text == '':
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _decode_csv_content(raw_bytes):
    encodings = ('utf-8-sig', 'gb18030', 'gbk')
    for encoding in encodings:
        try:
            return raw_bytes.decode(encoding)
        except Exception:
            continue
    raise ValueError('无法识别CSV文件编码，请导出为UTF-8或GBK后重试')


def _build_delivery_unique_hash(row):
    unique_key = '|'.join([
        str(row.get('成交日期') or '').strip(),
        str(row.get('成交时间') or '').strip(),
        str(row.get('证券代码') or '').strip(),
        str(row.get('操作') or '').strip(),
        str(row.get('成交编号') or '').strip(),
        str(row.get('合同编号') or '').strip(),
        str(row.get('成交数量') or '').strip(),
        str(row.get('成交价格') or '').strip(),
    ])
    return hashlib.sha1(unique_key.encode('utf-8')).hexdigest()


def _chunked(items, size):
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def _ensure_delivery_records_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_records (
            id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
            unique_hash VARCHAR(40) NOT NULL COMMENT '导入去重哈希',
            trade_date VARCHAR(8) COMMENT '成交日期，格式YYYYMMDD',
            trade_time VARCHAR(8) COMMENT '成交时间，格式HH:MM:SS',
            security_code VARCHAR(20) NOT NULL COMMENT '证券代码',
            security_name VARCHAR(64) COMMENT '证券名称',
            operation VARCHAR(32) COMMENT '操作（证券买入/证券卖出）',
            trade_quantity INT COMMENT '成交数量',
            trade_no VARCHAR(64) COMMENT '成交编号',
            trade_price DECIMAL(14, 3) COMMENT '成交价格',
            trade_amount DECIMAL(16, 3) COMMENT '成交金额',
            balance DECIMAL(16, 3) COMMENT '余额',
            stock_balance BIGINT COMMENT '股票余额',
            occurred_amount DECIMAL(16, 3) COMMENT '发生金额',
            commission DECIMAL(16, 3) COMMENT '佣金',
            stamp_duty DECIMAL(16, 3) COMMENT '印花税',
            other_fees DECIMAL(16, 3) COMMENT '其他杂费',
            current_amount DECIMAL(16, 3) COMMENT '本次金额',
            contract_no VARCHAR(64) COMMENT '合同编号',
            occurred_quantity INT COMMENT '发生数量',
            turnover_amount DECIMAL(16, 3) COMMENT '回转金额',
            net_commission DECIMAL(16, 3) COMMENT '净佣金',
            regulation_fee DECIMAL(16, 3) COMMENT '规费',
            transfer_fee DECIMAL(16, 3) COMMENT '过户费',
            market VARCHAR(32) COMMENT '交易市场',
            trade_datetime DATETIME COMMENT '成交时间戳',
            import_source VARCHAR(32) DEFAULT 'datong_csv' COMMENT '导入来源',
            raw_row_json TEXT COMMENT '原始行JSON',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            UNIQUE KEY uniq_delivery_hash (unique_hash),
            KEY idx_delivery_trade_datetime (trade_datetime),
            KEY idx_delivery_security_code (security_code),
            KEY idx_delivery_operation (operation)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='券商交割单记录表'
        """
    )


def import_delivery_csv(file_storage):
    """
    导入券商交割单（CSV）

    Args:
        file_storage: Flask request.files['file']

    Returns:
        dict: 导入结果
    """
    conn = None
    cursor = None
    try:
        if file_storage is None:
            return {'success': False, 'message': '未上传文件'}

        raw_bytes = file_storage.read()
        if not raw_bytes:
            return {'success': False, 'message': '文件内容为空'}
        if len(raw_bytes) > 8 * 1024 * 1024:
            return {'success': False, 'message': '文件过大，请控制在8MB以内'}

        csv_text = _decode_csv_content(raw_bytes)
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return {'success': False, 'message': 'CSV缺少表头'}

        required_headers = {'成交日期', '成交时间', '证券代码', '证券名称', '操作', '成交数量', '成交价格', '成交金额'}
        current_headers = {str(name or '').strip() for name in reader.fieldnames}
        missing_headers = required_headers - current_headers
        if missing_headers:
            return {'success': False, 'message': f"CSV缺少必要列: {', '.join(sorted(missing_headers))}"}

        parsed_rows = []
        invalid_rows = 0
        max_rows = 20000
        for row in reader:
            if len(parsed_rows) >= max_rows:
                break
            if not row:
                continue
            row = {str(k or '').strip(): v for k, v in row.items() if k is not None}

            operation_text = str(row.get('操作') or '').strip()
            if not operation_text:
                invalid_rows += 1
                continue
            is_sell = '卖' in operation_text
            is_buy = '买' in operation_text
            if not is_sell and not is_buy:
                invalid_rows += 1
                continue

            stock_code = _normalize_stock_code(row.get('证券代码'))
            if not stock_code:
                invalid_rows += 1
                continue

            trade_datetime = _parse_trade_datetime(row.get('成交日期'), row.get('成交时间'))
            trade_price = _parse_float(row.get('成交价格'))
            trade_amount = _parse_float(row.get('成交金额'))
            trade_quantity = _parse_int(row.get('成交数量'))
            if trade_datetime is None or trade_price is None or trade_amount is None:
                invalid_rows += 1
                continue

            unique_hash = _build_delivery_unique_hash(row)
            parsed_rows.append({
                'unique_hash': unique_hash,
                'trade_date': str(row.get('成交日期') or '').strip(),
                'trade_time': str(row.get('成交时间') or '').strip(),
                'stock_code': stock_code,
                'stock_name': str(row.get('证券名称') or '').strip(),
                'operation': operation_text,
                'trade_quantity': abs(trade_quantity) if trade_quantity is not None else None,
                'trade_no': str(row.get('成交编号') or '').strip(),
                'trade_price': trade_price,
                'trade_amount': trade_amount,
                'balance': _parse_float(row.get('余额')),
                'stock_balance': _parse_int(row.get('股票余额')),
                'occurred_amount': _parse_float(row.get('发生金额')),
                'commission': _parse_float(row.get('佣金')),
                'stamp_duty': _parse_float(row.get('印花税')),
                'other_fees': _parse_float(row.get('其他杂费')),
                'current_amount': _parse_float(row.get('本次金额')),
                'contract_no': str(row.get('合同编号') or '').strip(),
                'occurred_quantity': _parse_int(row.get('发生数量')),
                'turnover_amount': _parse_float(row.get('回转金额')),
                'net_commission': _parse_float(row.get('净佣金')),
                'regulation_fee': _parse_float(row.get('规费')),
                'transfer_fee': _parse_float(row.get('过户费')),
                'market': str(row.get('交易市场') or '').strip(),
                'trade_datetime': trade_datetime,
                'raw_row_json': json.dumps(row, ensure_ascii=False),
            })

        if not parsed_rows:
            return {'success': False, 'message': '未解析到有效成交记录，请检查文件格式'}

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_delivery_records_table(cursor)

        incoming_hashes = [item['unique_hash'] for item in parsed_rows]
        existing_hashes = set()
        for chunk in _chunked(incoming_hashes, 500):
            placeholders = ','.join(['%s'] * len(chunk))
            cursor.execute(
                f"SELECT unique_hash FROM delivery_records WHERE unique_hash IN ({placeholders})",
                tuple(chunk),
            )
            existing_hashes.update(row['unique_hash'] for row in cursor.fetchall())

        insert_rows = []
        for item in parsed_rows:
            if item['unique_hash'] in existing_hashes:
                continue
            insert_rows.append(
                (
                    item['unique_hash'],
                    item['trade_date'],
                    item['trade_time'],
                    item['stock_code'],
                    item['stock_name'],
                    item['operation'],
                    item['trade_quantity'],
                    item['trade_no'],
                    item['trade_price'],
                    item['trade_amount'],
                    item['balance'],
                    item['stock_balance'],
                    item['occurred_amount'],
                    item['commission'],
                    item['stamp_duty'],
                    item['other_fees'],
                    item['current_amount'],
                    item['contract_no'],
                    item['occurred_quantity'],
                    item['turnover_amount'],
                    item['net_commission'],
                    item['regulation_fee'],
                    item['transfer_fee'],
                    item['market'],
                    item['trade_datetime'],
                    item['raw_row_json'],
                )
            )

        inserted_count = 0
        if insert_rows:
            cursor.executemany(
                """
                INSERT INTO delivery_records (
                    unique_hash, trade_date, trade_time,
                    security_code, security_name, operation,
                    trade_quantity, trade_no, trade_price, trade_amount,
                    balance, stock_balance, occurred_amount,
                    commission, stamp_duty, other_fees, current_amount,
                    contract_no, occurred_quantity, turnover_amount,
                    net_commission, regulation_fee, transfer_fee,
                    market, trade_datetime, raw_row_json
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                insert_rows,
            )
            inserted_count = cursor.rowcount or 0
            conn.commit()

        skipped_existing = len(parsed_rows) - len(insert_rows)
        return {
            'success': True,
            'message': f'导入完成：新增 {inserted_count} 条，重复跳过 {skipped_existing} 条，无效 {invalid_rows} 条',
            'inserted': inserted_count,
            'skipped': skipped_existing,
            'invalid': invalid_rows,
            'total_parsed': len(parsed_rows),
        }
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"导入交割单失败: {e}")
        return {'success': False, 'message': f'导入失败: {str(e)}'}
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def get_delivery_records(stock_code=None, operation=None, limit=200):
    """查询交割单记录列表"""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        _ensure_delivery_records_table(cursor)

        sql = """
            SELECT
                id,
                unique_hash,
                trade_date,
                trade_time,
                security_code,
                security_name,
                operation,
                trade_quantity,
                trade_no,
                trade_price,
                trade_amount,
                balance,
                stock_balance,
                occurred_amount,
                commission,
                stamp_duty,
                other_fees,
                current_amount,
                contract_no,
                occurred_quantity,
                turnover_amount,
                net_commission,
                regulation_fee,
                transfer_fee,
                market,
                trade_datetime,
                created_at
            FROM delivery_records
            WHERE 1=1
        """
        params = []

        normalized_code = _normalize_stock_code(stock_code) if stock_code else ''
        if normalized_code:
            sql += " AND security_code = %s"
            params.append(normalized_code)

        if operation == 'buy':
            sql += " AND operation LIKE %s"
            params.append('%买%')
        elif operation == 'sell':
            sql += " AND operation LIKE %s"
            params.append('%卖%')
        elif operation:
            sql += " AND operation = %s"
            params.append(str(operation).strip())

        sql += " ORDER BY trade_datetime DESC, id DESC LIMIT %s"
        params.append(limit)

        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        for row in rows:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        return rows
    except Exception as e:
        print(f"查询交割单记录失败: {e}")
        return []
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass

