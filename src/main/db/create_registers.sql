CREATE TABLE `registers` (
  `id` varchar(255) NOT NULL,
  `proposal` bigint(20) NOT NULL,
  `accepted` bigint(20) NOT NULL,
  `val` longblob DEFAULT NULL,
  `replicas` blob DEFAULT NULL,
  `quorum_modified` tinyint(3) unsigned NOT NULL,
  `changed_replica` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8