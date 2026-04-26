-- =============================================
-- 大同证券交割单记录表（按CSV字段）
-- =============================================

CREATE TABLE IF NOT EXISTS delivery_records (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    unique_hash VARCHAR(40) NOT NULL COMMENT '导入去重哈希',

    -- 对应CSV字段
    trade_date VARCHAR(8) COMMENT '成交日期（YYYYMMDD）',
    trade_time VARCHAR(8) COMMENT '成交时间（HH:MM:SS）',
    security_code VARCHAR(20) NOT NULL COMMENT '证券代码',
    security_name VARCHAR(64) COMMENT '证券名称',
    operation VARCHAR(32) COMMENT '操作',
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

    -- 衍生字段
    trade_datetime DATETIME COMMENT '成交时间戳',
    import_source VARCHAR(32) DEFAULT 'datong_csv' COMMENT '导入来源',
    raw_row_json TEXT COMMENT '原始行JSON',

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    UNIQUE KEY uniq_delivery_hash (unique_hash),
    KEY idx_delivery_trade_datetime (trade_datetime),
    KEY idx_delivery_security_code (security_code),
    KEY idx_delivery_operation (operation)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='券商交割单记录表';
