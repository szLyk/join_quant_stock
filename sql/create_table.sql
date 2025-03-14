drop database IF EXISTS stock;
create database IF NOT EXISTS stock;

use stock;

DROP TABLE IF EXISTS stock.stock_industry;
CREATE TABLE if not EXISTS stock.stock_industry (
  `id` int NOT NULL AUTO_INCREMENT,
  `update_date` date DEFAULT NULL COMMENT '更新日期',
  `stock_code` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '证券代码',
  `stock_name` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '证券名称',
  `industry` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '所属行业',
  `industry_classification` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '所属行业类别',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_stock_code` (`stock_code`) USING BTREE
) comment = '股票行业表',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS stock.update_stock_record;
CREATE TABLE if not EXISTS stock.update_stock_record (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_name` varchar(255) DEFAULT NULL COMMENT '股票名称',
  `stock_code` varchar(255) DEFAULT NULL COMMENT '股票代码',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_stock_date` date DEFAULT '1990-01-01' COMMENT '日线更新记录',
  `update_stock_week` date DEFAULT '1990-01-01' COMMENT '周线更新记录',
  `update_stock_month` date DEFAULT '1990-01-01' COMMENT '月线更新记录',
  `update_stock_date_ma` date DEFAULT '1990-01-01' COMMENT '日均线更新记录',
  `update_stock_week_ma` date DEFAULT '1990-01-01' COMMENT '周均线更新记录',
  `update_stock_month_ma` date DEFAULT '1990-01-01' COMMENT '月均线更新记录',
  PRIMARY KEY (`id`),
  UNIQUE KEY `u_index_stock_code` (`stock_code`) USING BTREE,
  KEY `index_stock_name` (`stock_name`) USING BTREE
)  comment = '股票更新记录表',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

drop table if exists stock.stock_history_date_price;
CREATE TABLE if not exists stock.stock_history_date_price (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) NOT NULL COMMENT '证券代码',
  `stock_id` int NOT NULL,
  `stock_date` date NOT NULL COMMENT '交易所行情日期',
  `open_price` decimal(10,4) DEFAULT NULL COMMENT '开盘价',
  `high_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `low_price` decimal(10,4) DEFAULT NULL COMMENT '最低价',
  `close_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `trading_volume` decimal(20,4) DEFAULT NULL COMMENT '成交量（累计 单位：股）',
  `trading_amount` decimal(20,4) DEFAULT NULL COMMENT '成交额（单位：人民币元）',
  `adjust_flag` tinyint DEFAULT NULL COMMENT '复权状态(1：后复权， 2：前复权，3：不复权）',
  `turn` decimal(20,4) DEFAULT NULL COMMENT '换手率',
  `tradestatus` tinyint DEFAULT NULL COMMENT '交易状态(1：正常交易 0：停牌）',
  `Increase_and_decrease` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅（百分比）',
  `if_st` tinyint DEFAULT NULL COMMENT '是否ST股，1是，0否',
  `pb_ratio` decimal(20,4) DEFAULT NULL COMMENT '市净率((指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具))',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_stock_date_price` (`stock_code`,`stock_date`) USING BTREE,
  KEY `index_tradestatus` (`tradestatus`) USING BTREE,
  KEY `index_stock_date` (`stock_date`) USING BTREE
)comment = '股票日线价格表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS stock.stock_history_week_price;
CREATE TABLE IF NOT EXISTS stock.stock_history_week_price (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) NOT NULL COMMENT '证券代码',
  `stock_id` varchar(20) NOT NULL,
  `stock_date` date NOT NULL COMMENT '交易所行情日期',
  `open_price` decimal(10,4) DEFAULT NULL COMMENT '开盘价',
  `high_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `low_price` decimal(10,4) DEFAULT NULL COMMENT '最低价',
  `close_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `trading_volume` decimal(20,4) DEFAULT NULL COMMENT '成交量（累计 单位：股）',
  `trading_amount` decimal(20,4) DEFAULT NULL COMMENT '成交额（单位：人民币元）',
  `adjust_flag` tinyint DEFAULT NULL COMMENT '复权状态(1：后复权， 2：前复权，3：不复权）',
  `turn` decimal(20,4) DEFAULT NULL COMMENT '换手率',
  `Increase_and_decrease` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅（百分比）',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_stock_date_price` (`stock_code`,`stock_date`) USING BTREE
) comment = '股票周线价格表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS stock.stock_history_month_price;
CREATE TABLE IF NOT EXISTS stock.stock_history_month_price (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) NOT NULL COMMENT '证券代码',
  `stock_id` varchar(20) NOT NULL,
  `stock_date` date NOT NULL COMMENT '交易所行情日期',
  `open_price` decimal(10,4) DEFAULT NULL COMMENT '开盘价',
  `high_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `low_price` decimal(10,4) DEFAULT NULL COMMENT '最低价',
  `close_price` decimal(10,4) DEFAULT NULL COMMENT '收盘价',
  `trading_volume` decimal(20,4) DEFAULT NULL COMMENT '成交量（累计 单位：股）',
  `trading_amount` decimal(20,4) DEFAULT NULL COMMENT '成交额（单位：人民币元）',
  `adjust_flag` tinyint DEFAULT NULL COMMENT '复权状态(1：后复权， 2：前复权，3：不复权）',
  `turn` decimal(20,4) DEFAULT NULL COMMENT '换手率',
  `Increase_and_decrease` decimal(10,4) DEFAULT NULL COMMENT '涨跌幅（百分比）',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_stock_date_price` (`stock_code`,`stock_date`) USING BTREE
)comment = '股票月线价格表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS stock.date_stock_moving_average_table;
CREATE TABLE IF NOT EXISTS stock.date_stock_moving_average_table (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) DEFAULT NULL,
  `stock_name` varchar(20) DEFAULT NULL,
  `stock_date` date DEFAULT NULL,
  `stock_week_date` date DEFAULT NULL COMMENT 'A股周线交易日',
  `stock_month_date` date DEFAULT NULL COMMENT 'A股月线交易日',
  `close_price` decimal(20,4) DEFAULT NULL COMMENT '收盘价',
  `stock_ma3` decimal(20,4) DEFAULT NULL COMMENT '3日均线',
  `stock_ma5` decimal(20,4) DEFAULT NULL COMMENT '5日均线',
  `stock_ma6` decimal(20,4) DEFAULT NULL COMMENT '6日均线',
  `stock_ma7` decimal(20,4) DEFAULT NULL COMMENT '7日均线',
  `stock_ma9` decimal(20,4) DEFAULT NULL COMMENT '9日均线',
  `stock_ma10` decimal(20,4) DEFAULT NULL COMMENT '10日均线',
  `stock_ma12` decimal(20,4) DEFAULT NULL COMMENT '12日均线',
  `stock_ma20` decimal(20,4) DEFAULT NULL COMMENT '20日均线',
  `stock_ma24` decimal(20,4) DEFAULT NULL COMMENT '24日均线',
  `stock_ma26` decimal(20,4) DEFAULT NULL COMMENT '26日均线',
  `stock_ma30` decimal(20,4) DEFAULT NULL COMMENT '30日均线',
  `stock_ma60` decimal(20,4) DEFAULT NULL COMMENT '60日均线',
  `stock_ma70` decimal(20,4) DEFAULT NULL COMMENT '70日均线',
  `stock_ma125` decimal(20,4) DEFAULT NULL COMMENT '125日均线',
  `stock_ma250` decimal(20,4) DEFAULT NULL COMMENT '250日均线',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_index_stock_code_and_date` (`stock_code`,`stock_date`) USING BTREE,
  KEY `index_stock_name` (`stock_name`) USING BTREE,
  KEY `index_stock_date` (`stock_date`) USING BTREE
)comment = '股票日线均线表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS stock.week_stock_moving_average_table;
CREATE TABLE IF NOT EXISTS stock.week_stock_moving_average_table (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) DEFAULT NULL,
  `stock_name` varchar(20) DEFAULT NULL,
  `stock_date` date DEFAULT NULL,
  `stock_week_date` date DEFAULT NULL COMMENT 'A股周线交易日',
  `stock_month_date` date DEFAULT NULL COMMENT 'A股月线交易日',
  `close_price` decimal(20,4) DEFAULT NULL COMMENT '收盘价',
  `stock_ma3` decimal(20,4) DEFAULT NULL COMMENT '3日均线',
  `stock_ma5` decimal(20,4) DEFAULT NULL COMMENT '5日均线',
  `stock_ma6` decimal(20,4) DEFAULT NULL COMMENT '6日均线',
  `stock_ma7` decimal(20,4) DEFAULT NULL COMMENT '7日均线',
  `stock_ma9` decimal(20,4) DEFAULT NULL COMMENT '9日均线',
  `stock_ma10` decimal(20,4) DEFAULT NULL COMMENT '10日均线',
  `stock_ma12` decimal(20,4) DEFAULT NULL COMMENT '12日均线',
  `stock_ma20` decimal(20,4) DEFAULT NULL COMMENT '20日均线',
  `stock_ma24` decimal(20,4) DEFAULT NULL COMMENT '24日均线',
  `stock_ma26` decimal(20,4) DEFAULT NULL COMMENT '26日均线',
  `stock_ma30` decimal(20,4) DEFAULT NULL COMMENT '30日均线',
  `stock_ma60` decimal(20,4) DEFAULT NULL COMMENT '60日均线',
  `stock_ma70` decimal(20,4) DEFAULT NULL COMMENT '70日均线',
  `stock_ma125` decimal(20,4) DEFAULT NULL COMMENT '125日均线',
  `stock_ma250` decimal(20,4) DEFAULT NULL COMMENT '250日均线',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_index_stock_code_and_date` (`stock_code`,`stock_date`) USING BTREE,
  KEY `index_stock_name` (`stock_name`) USING BTREE,
  KEY `index_stock_date` (`stock_date`) USING BTREE
)comment = '股票周线均线表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS stock.month_stock_moving_average_table;
CREATE TABLE IF NOT EXISTS stock.month_stock_moving_average_table (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) DEFAULT NULL,
  `stock_name` varchar(20) DEFAULT NULL,
  `stock_date` date DEFAULT NULL,
  `stock_week_date` date DEFAULT NULL COMMENT 'A股周线交易日',
  `stock_month_date` date DEFAULT NULL COMMENT 'A股月线交易日',
  `close_price` decimal(20,4) DEFAULT NULL COMMENT '收盘价',
  `stock_ma3` decimal(20,4) DEFAULT NULL COMMENT '3日均线',
  `stock_ma5` decimal(20,4) DEFAULT NULL COMMENT '5日均线',
  `stock_ma6` decimal(20,4) DEFAULT NULL COMMENT '6日均线',
  `stock_ma7` decimal(20,4) DEFAULT NULL COMMENT '7日均线',
  `stock_ma9` decimal(20,4) DEFAULT NULL COMMENT '9日均线',
  `stock_ma10` decimal(20,4) DEFAULT NULL COMMENT '10日均线',
  `stock_ma12` decimal(20,4) DEFAULT NULL COMMENT '12日均线',
  `stock_ma20` decimal(20,4) DEFAULT NULL COMMENT '20日均线',
  `stock_ma24` decimal(20,4) DEFAULT NULL COMMENT '24日均线',
  `stock_ma26` decimal(20,4) DEFAULT NULL COMMENT '26日均线',
  `stock_ma30` decimal(20,4) DEFAULT NULL COMMENT '30日均线',
  `stock_ma60` decimal(20,4) DEFAULT NULL COMMENT '60日均线',
  `stock_ma70` decimal(20,4) DEFAULT NULL COMMENT '70日均线',
  `stock_ma125` decimal(20,4) DEFAULT NULL COMMENT '125日均线',
  `stock_ma250` decimal(20,4) DEFAULT NULL COMMENT '250日均线',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_index_stock_code_and_date` (`stock_code`,`stock_date`) USING BTREE,
  KEY `index_stock_name` (`stock_name`) USING BTREE,
  KEY `index_stock_date` (`stock_date`) USING BTREE
)comment = '股票月线均线表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS stock.date_stock_macd;
CREATE TABLE IF NOT EXISTS stock.date_stock_macd (
  `id` int NOT NULL AUTO_INCREMENT,
  `stock_code` varchar(20) DEFAULT NULL comment '股票代码',
  `stock_name` varchar(20) DEFAULT NULL comment '股票名称',
  `stock_date` date DEFAULT NULL comment '股票交易日',
  `close_price` decimal(20,4) DEFAULT NULL COMMENT '收盘价',
  `ema_12` decimal(20,4) DEFAULT NULL COMMENT 'macd中ema12指标',
  `ema_26` decimal(20,4) DEFAULT NULL COMMENT 'macd中ema26指标',
  `diff` decimal(20,4) DEFAULT NULL COMMENT '',
  `dea` decimal(20,4) DEFAULT NULL COMMENT '',
	`macd` decimal(10,4) DEFAULT NULL COMMENT 'macd',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_index_stock_code_and_date` (`stock_code`,`stock_date`) USING BTREE,
  KEY `index_stock_name` (`stock_name`) USING BTREE,
  KEY `index_stock_date` (`stock_date`) USING BTREE
)comment = '股票日线MACD表（后复权）',
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;