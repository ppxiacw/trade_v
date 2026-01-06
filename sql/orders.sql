-- =============================================
-- 订单系统数据库设计
-- =============================================

-- 订单表
CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '订单ID',
    order_no VARCHAR(32) NOT NULL UNIQUE COMMENT '订单编号',
    
    -- 股票信息
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(50) COMMENT '股票名称',
    
    -- 价格信息
    buy_price DECIMAL(10, 2) NOT NULL COMMENT '买入价格',
    current_price DECIMAL(10, 2) COMMENT '下单时现价',
    take_profit_price DECIMAL(10, 2) COMMENT '止盈价格（第一压力位）',
    stop_loss_price DECIMAL(10, 2) COMMENT '止损价格',
    
    -- 金额和股数
    plan_amount DECIMAL(12, 2) COMMENT '计划买入金额',
    buy_shares INT COMMENT '买入股数',
    actual_amount DECIMAL(12, 2) COMMENT '实际买入金额',
    total_capital DECIMAL(12, 2) COMMENT '总资金',
    
    -- 盈亏预估
    expected_profit DECIMAL(12, 2) COMMENT '预计盈利',
    expected_loss DECIMAL(12, 2) COMMENT '预计亏损',
    profit_loss_ratio DECIMAL(5, 2) COMMENT '盈亏比',
    
    -- 下单理由
    order_reason VARCHAR(50) COMMENT '下单理由代码',
    order_reason_label VARCHAR(100) COMMENT '下单理由描述',
    take_profit_reason VARCHAR(50) COMMENT '止盈理由',
    stop_loss_reason VARCHAR(50) COMMENT '止损理由',
    
    -- 仓位评估
    conditions_json TEXT COMMENT '评估条件JSON',
    recommended_position INT COMMENT '推荐仓位百分比',
    total_score INT COMMENT '评估总得分',
    score_rate DECIMAL(5, 2) COMMENT '得分率',
    
    -- 订单状态
    status ENUM('pending', 'executed', 'partial', 'cancelled', 'closed') DEFAULT 'pending' COMMENT '订单状态',
    
    -- 时间戳
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    executed_at DATETIME COMMENT '执行时间',
    closed_at DATETIME COMMENT '平仓时间',
    
    -- 备注
    remarks TEXT COMMENT '备注',
    
    INDEX idx_stock_code (stock_code),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_order_no (order_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表';


-- 订单状态变更日志表
CREATE TABLE IF NOT EXISTS order_status_logs (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
    order_id INT NOT NULL COMMENT '订单ID',
    order_no VARCHAR(32) NOT NULL COMMENT '订单编号',
    
    old_status VARCHAR(20) COMMENT '原状态',
    new_status VARCHAR(20) NOT NULL COMMENT '新状态',
    
    note TEXT COMMENT '变更说明',
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    
    INDEX idx_order_id (order_id),
    INDEX idx_order_no (order_no),
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单状态变更日志';


-- =============================================
-- 示例查询
-- =============================================

-- 查询所有待执行订单
-- SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC;

-- 查询某只股票的订单历史
-- SELECT * FROM orders WHERE stock_code = 'sh600519' ORDER BY created_at DESC;

-- 查询订单状态变更历史
-- SELECT l.*, o.stock_code, o.stock_name 
-- FROM order_status_logs l 
-- JOIN orders o ON l.order_id = o.id 
-- WHERE l.order_no = 'ORD20260106123456789'
-- ORDER BY l.created_at;

