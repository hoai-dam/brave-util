
CREATE SCHEMA IF NOT EXISTS `tala_acp`;

USE `tala_acp`;

CREATE TABLE IF NOT EXISTS `users` (
     `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
     `name` varchar(255) NOT NULL,
     `email` varchar(255) NOT NULL,
     `is_email_verified` tinyint(1) DEFAULT '0',
     `password` varchar(255) NOT NULL,
     `is_required_change_password` tinyint(1) DEFAULT '0',
     `change_password_at` datetime DEFAULT NULL,
     `status` tinyint(1) NOT NULL DEFAULT '0',
     `remember_token` varchar(100) DEFAULT NULL,
     `created_at` timestamp NULL DEFAULT NULL,
     `updated_at` timestamp NULL DEFAULT NULL,
     `seller_id` int(11) NOT NULL DEFAULT '0',
     `is_triggered` tinyint(1) DEFAULT '0',
     `phone_number` varchar(11) DEFAULT NULL,
     PRIMARY KEY (`id`),
     UNIQUE KEY `users_email_unique` (`email`) USING BTREE,
     KEY `seller_idx` (`seller_id`) USING BTREE
);