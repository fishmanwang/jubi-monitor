drop table if exists jb_xas_ticker;
CREATE TABLE `jb_xas_ticker` (
  `pk` int(11) NOT NULL,
  `high` decimal(18,6) NOT NULL,
  `low` decimal(18,6) NOT NULL,
  `buy` decimal(18,6) NOT NULL,
  `sell` decimal(18,6) NOT NULL,
  `last` decimal(18,6) NOT NULL,
  `vol` decimal(18,6) NOT NULL,
  `volume` decimal(18,6) NOT NULL,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS jb_coin_increase;
CREATE TABLE jb_coin_increase(
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  coin VARCHAR(16) NOT NULL,
  pk INT NOT NULL,
  rate decimal(18,6) NOT NULL DEFAULT 0 COMMENT '涨幅',
  UNIQUE(pk, coin)
) COMMENT = '币值涨幅';