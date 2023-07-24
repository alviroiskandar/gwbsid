-- Adminer 4.8.1 MySQL 8.0.33 dump

SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `fic_mis_date` date DEFAULT NULL,
  `CIF_iBSM` bigint DEFAULT NULL,
  `CIF_AB` varchar(20) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `NamaLengkap_CIF` varchar(50) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Nama_Rekening` varchar(50) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `nomor_rekening` bigint DEFAULT NULL,
  `NomorRekening_AB` varchar(16) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `SaldoAcy` float DEFAULT NULL,
  `SaldoLcy` float DEFAULT NULL,
  `BranchCode` char(9) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `CcyCode` char(3) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Local_TipeNasabah` char(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `product_code` bigint DEFAULT NULL,
  `FundProductDesc` varchar(35) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `FundProduct_Level3` char(8) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `TanggalBuka` bigint DEFAULT NULL,
  `SpesialNisbah` float DEFAULT NULL,
  `AccountOfficerCode` varchar(8) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `AccountOfficerName` varchar(35) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `group` char(3) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `event_Code` bigint DEFAULT NULL,
  `Prog_Code` varchar(5) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Priority_flag` char(1) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `CIF_ACCTOFF` varchar(8) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `REFF_OUTSORCE` varchar(8) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Saldo_Rata2` float DEFAULT NULL,
  `Job_Title` varchar(48) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Occupation` varchar(35) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `No_HP` varchar(17) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Alamat_Nasabah` varchar(50) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Instansi` varchar(35) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Status_Pernikahan` varchar(8) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Agama` bigint DEFAULT NULL,
  `Pendidikan` bigint DEFAULT NULL,
  `Umur` bigint DEFAULT NULL,
  `Jenis_Kelamin` varchar(6) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `Tgl_Buka_CIF` bigint DEFAULT NULL,
  `KD_Outlet_Buka_CIF` char(9) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `GROUP_REAL` char(3) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `nomor_rekening` (`nomor_rekening`),
  KEY `NomorRekening_AB` (`NomorRekening_AB`),
  KEY `SaldoAcy` (`SaldoAcy`),
  KEY `SaldoLcy` (`SaldoLcy`),
  KEY `Saldo_Rata2` (`Saldo_Rata2`),
  KEY `Umur` (`Umur`),
  KEY `TanggalBuka` (`TanggalBuka`),
  KEY `Jenis_Kelamin` (`Jenis_Kelamin`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci;


-- 2023-07-24 20:19:27
