DROP TABLE IF EXISTS jb_coin;
CREATE TABLE jb_coin(
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `code` VARCHAR(16) NOT NULL,
  `name` VARCHAR(32) NOT NULL
) COMMENT='虚拟币信息表';

DROP TABLE IF EXISTS jb_coin_ticker;
CREATE TABLE `jb_coin_ticker` (
  `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `pk` INT(11) NOT NULL,
  `coin` VARCHAR(16) NOT NULL,
  `price` DECIMAL(18,6) NOT NULL DEFAULT 0,
  UNIQUE(`pk`, `coin`)
) COMMENT='虚拟币行情表';

DROP TABLE IF EXISTS jb_coin_rate;
CREATE TABLE `jb_coin_rate` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `coin` VARCHAR(16) NOT NULL,
  `pk` INT(11) NOT NULL,
  `rate` DECIMAL(18,6) NOT NULL DEFAULT '0.000000' COMMENT '涨幅',
  PRIMARY KEY (`id`),
  UNIQUE KEY `pk` (`pk`,`coin`)
) ENGINE=INNODB AUTO_INCREMENT=415395 DEFAULT CHARSET=utf8 COMMENT='虚拟币涨幅';

DROP TABLE IF EXISTS jb_coin_depth;
CREATE TABLE jb_coin_depth(
  id INT PRIMARY KEY AUTO_INCREMENT,
  pk INT NOT NULL,
  coin VARCHAR(16) NOT NULL,
  price DECIMAL(18,6) NOT NULL,
  asks TEXT NOT NULL DEFAULT '' COMMENT '卖',
  bids TEXT NOT NULL DEFAULT '' COMMENT '买',
  UNIQUE(pk, coin)
) COMMENT '虚拟币深度表';

DROP TABLE IF EXISTS jb_price_notify;
CREATE TABLE jb_price_notify(
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL COMMENT '用户',
  coin VARCHAR(16) NOT NULL COMMENT '币',
  price DECIMAL(18,6) NOT NULL COMMENT '价格。正为涨；负为跌',
  create_time DATETIME,
  update_time DATETIME,
  INDEX idx_user(user_id),
  INDEX idx_coin_price(coin, price)
) COMMENT = '价格提醒表';

DROP TABLE IF EXISTS jb_price_rate_notify;
CREATE TABLE `jb_price_rate_notify` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `user_id` INT(11) NOT NULL,
  `coin` VARCHAR(16) NOT NULL,
  `rate` INT NOT NULL DEFAULT '5' COMMENT '监控的涨跌幅度',
  `create_time` DATETIME DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`coin`)
) ENGINE=INNODB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='价格涨幅提醒';

DROP TABLE IF EXISTS jb_price_wave_notify;
CREATE TABLE `jb_price_wave_notify` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `coin` varchar(16) NOT NULL,
  `span` int(11) NOT NULL DEFAULT '10' COMMENT '时间间隔',
  `rate` decimal(10,2) NOT NULL DEFAULT '2.00' COMMENT '波动幅',
  `create_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`coin`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8 COMMENT='价格波动提醒';

DROP TABLE IF EXISTS jb_price_rate_monitor_setting;
CREATE TABLE jb_price_rate_monitor_setting(
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `rate` INT NOT NULL DEFAULT 5 COMMENT '可监控幅度',
  `create_time` DATE,
  PRIMARY KEY (`id`),
  UNIQUE KEY(`rate`)
) COMMENT = '配置可监控幅度';