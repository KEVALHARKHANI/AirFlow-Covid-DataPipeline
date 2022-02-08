CREATE DATABASE Covid;
USE Covid;

CREATE TABLE `Covid_dates` (
  `id` int NOT NULL AUTO_INCREMENT,
  `covid_date` date NOT NULL DEFAULT (curdate()),
  `created` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `Covid_table` (
	`id` INT NOT NULL auto_increment,
  `Country` text,
  `TotalCases` bigint DEFAULT NULL,
  `NewCases` double DEFAULT NULL,
  `TotalDeaths` double DEFAULT NULL,
  `NewDeaths` double DEFAULT NULL,
  `TotalRecovered` double DEFAULT NULL,
  `NewRecovered` double DEFAULT NULL,
  `ActiveCases` double DEFAULT NULL,
  `Serious` double DEFAULT NULL,
  `Tot_Cases_per_1M_pop` double DEFAULT NULL,
  `Deaths_per_1M_pop` double DEFAULT NULL,
  `TotalTests` double DEFAULT NULL,
  `Tests_per__1M_pop_` double DEFAULT NULL,
  `Population` double DEFAULT NULL,
  `Continent` text,
  `1_Caseevery_X_ppl` double DEFAULT NULL,
  `1_Deathevery_X_ppl` double DEFAULT NULL,
  `1_Testevery_X_ppl` double DEFAULT NULL,
  `New_Cases_per_1M_pop` double DEFAULT NULL,
  `New_Deaths_per_1M_pop` double DEFAULT NULL,
  `Active_Cases_per_1M_pop` double DEFAULT NULL,
  `date_id` int NOT NULL,
  KEY `date_id` (`date_id`),
  primary key(id),
  CONSTRAINT `covid_table_ibfk_1` FOREIGN KEY (`date_id`) REFERENCES `Covid_dates` (`id`)
);