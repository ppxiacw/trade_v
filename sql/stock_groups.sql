-- =============================================
-- 股票分组数据库设计
-- =============================================

-- 股票分组表
CREATE TABLE IF NOT EXISTS stock_groups (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '分组ID',
    group_name VARCHAR(50) NOT NULL COMMENT '分组名称',
    group_code VARCHAR(50) NOT NULL UNIQUE COMMENT '分组代码（唯一标识）',
    description VARCHAR(200) COMMENT '分组描述',
    color VARCHAR(20) DEFAULT '#667eea' COMMENT '分组颜色',
    sort_order INT DEFAULT 0 COMMENT '排序顺序',
    is_default TINYINT(1) DEFAULT 0 COMMENT '是否默认分组',
    is_active TINYINT(1) DEFAULT 1 COMMENT '是否启用',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    INDEX idx_group_code (group_code),
    INDEX idx_sort_order (sort_order),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票分组表';


-- 分组股票关联表
CREATE TABLE IF NOT EXISTS stock_group_items (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'ID',
    group_id INT NOT NULL COMMENT '分组ID',
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(50) COMMENT '股票名称',
    sort_order INT DEFAULT 0 COMMENT '排序顺序',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',

    UNIQUE KEY uk_group_stock (group_id, stock_code),
    INDEX idx_group_id (group_id),
    INDEX idx_stock_code (stock_code),

    FOREIGN KEY (group_id) REFERENCES stock_groups(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分组股票关联表';


-- =============================================
-- 初始数据：默认分组
-- =============================================

-- 插入默认常用指数分组
INSERT INTO stock_groups (group_name, group_code, description, color, sort_order, is_default) VALUES
('常用指数', 'common_index', '常用的大盘指数', '#667eea', 1, 1),
('自选股', 'favorites', '我的自选股票', '#f56c6c', 2, 0),
('关注板块', 'sectors', '关注的行业板块', '#e6a23c', 3, 0);

-- 为常用指数分组添加默认股票
INSERT INTO stock_group_items (group_id, stock_code, stock_name, sort_order) VALUES
(1, 'sh000001', '上证指数', 1),
(1, 'sz399006', '创业板指', 2),
(1, 'sh000852', '中证1000', 3),
(1, 'sh000300', '沪深300', 4);


-- =============================================
-- 示例查询
-- =============================================

-- 查询所有分组及其股票数量
-- SELECT g.*, COUNT(i.id) as stock_count
-- FROM stock_groups g
-- LEFT JOIN stock_group_items i ON g.id = i.group_id
-- WHERE g.is_active = 1
-- GROUP BY g.id
-- ORDER BY g.sort_order;

-- 查询某个分组的所有股票
-- SELECT i.*, g.group_name
-- FROM stock_group_items i
-- JOIN stock_groups g ON i.group_id = g.id
-- WHERE g.group_code = 'common_index'
-- ORDER BY i.sort_order;

-- 查询某只股票所属的所有分组
-- SELECT g.*
-- FROM stock_groups g
-- JOIN stock_group_items i ON g.id = i.group_id
-- WHERE i.stock_code = 'sh000001';

