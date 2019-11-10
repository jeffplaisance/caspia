CREATE TABLE `log01` (
  `id` bigint(20) NOT NULL,
  `proposal` int(11) NOT NULL,
  `accepted` int(11) NOT NULL,
  `val` longblob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8