/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-11.2.6-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: atis
-- ------------------------------------------------------
-- Server version	11.2.6-MariaDB-ubu2204

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `aircraft`
--

DROP TABLE IF EXISTS `aircraft`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `aircraft` (
  `aircraft_code` varchar(3) DEFAULT NULL,
  `aircraft_description` varchar(50) DEFAULT NULL,
  `manufacturer` varchar(30) DEFAULT NULL,
  `basic_type` varchar(30) DEFAULT NULL,
  `engines` int(11) DEFAULT NULL,
  `propulsion` varchar(10) DEFAULT NULL,
  `wide_body` varchar(3) DEFAULT NULL,
  `wing_span` int(11) DEFAULT NULL,
  `length` int(11) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
  `capacity` int(11) DEFAULT NULL,
  `pay_load` int(11) DEFAULT NULL,
  `cruising_speed` int(11) DEFAULT NULL,
  `range_miles` int(11) DEFAULT NULL,
  `pressurized` varchar(3) DEFAULT NULL,
  KEY `aircraft_aircraft_description` (`aircraft_description`),
  KEY `aircraft_basic_type` (`basic_type`),
  KEY `aircraft_propulsion` (`propulsion`),
  KEY `aircraft_manufacturer` (`manufacturer`),
  KEY `aircraft_code` (`aircraft_code`),
  KEY `engines` (`engines`),
  KEY `wide_body` (`wide_body`),
  KEY `wing_span` (`wing_span`),
  KEY `length` (`length`),
  KEY `weight` (`weight`),
  KEY `capacity` (`capacity`),
  KEY `pay_load` (`pay_load`),
  KEY `cruising_speed` (`cruising_speed`),
  KEY `range_miles` (`range_miles`),
  KEY `pressurized` (`pressurized`),
  KEY `aircraft_description` (`aircraft_description`),
  KEY `manufacturer` (`manufacturer`),
  KEY `basic_type` (`basic_type`),
  KEY `aircraft_code_2` (`aircraft_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `airline`
--

DROP TABLE IF EXISTS `airline`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `airline` (
  `airline_code` varchar(2) DEFAULT NULL,
  `airline_name` text DEFAULT NULL,
  `note` text DEFAULT NULL,
  KEY `airline_airline_name` (`airline_name`(100)),
  KEY `airline_code` (`airline_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `airport`
--

DROP TABLE IF EXISTS `airport`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `airport` (
  `airport_code` varchar(3) DEFAULT NULL,
  `airport_name` text DEFAULT NULL,
  `airport_location` text DEFAULT NULL,
  `state_code` varchar(2) DEFAULT NULL,
  `country_name` varchar(6) DEFAULT NULL,
  `time_zone_code` varchar(3) DEFAULT NULL,
  `minimum_connect_time` int(11) DEFAULT NULL,
  KEY `airport_airport_code` (`airport_code`),
  KEY `airport_airport_name` (`airport_name`(100)),
  KEY `airport_airport_location` (`airport_location`(100)),
  KEY `state_code` (`state_code`),
  KEY `country_name` (`country_name`),
  KEY `time_zone_code` (`time_zone_code`),
  KEY `minimum_connect_time` (`minimum_connect_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `airport_service`
--

DROP TABLE IF EXISTS `airport_service`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `airport_service` (
  `city_code` varchar(4) DEFAULT NULL,
  `airport_code` varchar(3) DEFAULT NULL,
  `miles_distant` int(11) DEFAULT NULL,
  `direction` varchar(2) DEFAULT NULL,
  `minutes_distant` int(11) DEFAULT NULL,
  KEY `airport_code` (`airport_code`),
  KEY `city_code` (`city_code`),
  KEY `miles_distant` (`miles_distant`),
  KEY `minutes_distant` (`minutes_distant`),
  KEY `direction` (`direction`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `city`
--

DROP TABLE IF EXISTS `city`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `city` (
  `city_code` varchar(4) DEFAULT NULL,
  `city_name` varchar(18) DEFAULT NULL,
  `state_code` varchar(2) DEFAULT NULL,
  `country_name` varchar(6) DEFAULT NULL,
  `time_zone_code` varchar(3) DEFAULT NULL,
  KEY `city_state_code` (`state_code`),
  KEY `city_code` (`city_code`),
  KEY `city_name` (`city_name`),
  KEY `country_name` (`country_name`),
  KEY `time_zone_code` (`time_zone_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `class_of_service`
--

DROP TABLE IF EXISTS `class_of_service`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `class_of_service` (
  `booking_class` varchar(2) NOT NULL DEFAULT '',
  `rank` int(11) DEFAULT NULL,
  `class_description` text DEFAULT NULL,
  PRIMARY KEY (`booking_class`),
  KEY `rank` (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `code_description`
--

DROP TABLE IF EXISTS `code_description`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `code_description` (
  `code` varchar(4) NOT NULL DEFAULT '',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `compartment_class`
--

DROP TABLE IF EXISTS `compartment_class`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `compartment_class` (
  `compartment` varchar(5) DEFAULT NULL,
  `class_type` varchar(8) DEFAULT NULL,
  KEY `compartment` (`compartment`),
  KEY `class_type` (`class_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `date_day`
--

DROP TABLE IF EXISTS `date_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `date_day` (
  `month_number` int(11) DEFAULT NULL,
  `day_number` int(11) DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  `day_name` varchar(10) DEFAULT NULL,
  KEY `month_number` (`month_number`),
  KEY `day_number` (`day_number`),
  KEY `year` (`year`),
  KEY `day_name` (`day_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `days`
--

DROP TABLE IF EXISTS `days`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `days` (
  `days_code` varchar(20) DEFAULT NULL,
  `day_name` varchar(10) DEFAULT NULL,
  KEY `days_code` (`days_code`),
  KEY `day_name` (`day_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dual_carrier`
--

DROP TABLE IF EXISTS `dual_carrier`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dual_carrier` (
  `main_airline` varchar(2) DEFAULT NULL,
  `low_flight_number` int(11) DEFAULT NULL,
  `high_flight_number` int(11) DEFAULT NULL,
  `dual_airline` varchar(2) DEFAULT NULL,
  `service_name` text DEFAULT NULL,
  KEY `main_airline` (`main_airline`),
  KEY `low_flight_number` (`low_flight_number`),
  KEY `high_flight_number` (`high_flight_number`),
  KEY `dual_airline` (`dual_airline`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `equipment_sequence`
--

DROP TABLE IF EXISTS `equipment_sequence`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `equipment_sequence` (
  `aircraft_code_sequence` varchar(12) DEFAULT NULL,
  `aircraft_code` varchar(3) DEFAULT NULL,
  KEY `aircraft_code` (`aircraft_code`),
  KEY `aircraft_code_sequence` (`aircraft_code_sequence`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fare`
--

DROP TABLE IF EXISTS `fare`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fare` (
  `fare_id` int(11) NOT NULL DEFAULT 0,
  `from_airport` varchar(3) DEFAULT NULL,
  `to_airport` varchar(3) DEFAULT NULL,
  `fare_basis_code` text DEFAULT NULL,
  `fare_airline` text DEFAULT NULL,
  `restriction_code` text DEFAULT NULL,
  `one_direction_cost` int(11) DEFAULT NULL,
  `round_trip_cost` int(11) DEFAULT NULL,
  `round_trip_required` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`fare_id`),
  KEY `fare_restriction_code` (`restriction_code`(100)),
  KEY `fare_fare_basis_code` (`fare_basis_code`(100)),
  KEY `fare_fare_airline` (`fare_airline`(100)),
  KEY `one_direction_cost` (`one_direction_cost`),
  KEY `round_trip_cost` (`round_trip_cost`),
  KEY `round_trip_required` (`round_trip_required`),
  KEY `from_airport` (`from_airport`),
  KEY `to_airport` (`to_airport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fare_basis`
--

DROP TABLE IF EXISTS `fare_basis`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fare_basis` (
  `fare_basis_code` text DEFAULT NULL,
  `booking_class` text DEFAULT NULL,
  `class_type` text DEFAULT NULL,
  `premium` text DEFAULT NULL,
  `economy` text DEFAULT NULL,
  `discounted` text DEFAULT NULL,
  `night` text DEFAULT NULL,
  `season` text DEFAULT NULL,
  `basis_days` text DEFAULT NULL,
  KEY `fare_basis_premium` (`premium`(100)),
  KEY `fare_basis_class_type` (`class_type`(100)),
  KEY `fare_basis_season` (`season`(100)),
  KEY `fare_basis_booking_class` (`booking_class`(100)),
  KEY `fare_basis_night` (`night`(100)),
  KEY `fare_basis_discounted` (`discounted`(100)),
  KEY `fare_basis_fare_basis_code` (`fare_basis_code`(100)),
  KEY `fare_basis_economy` (`economy`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `flight`
--

DROP TABLE IF EXISTS `flight`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `flight` (
  `flight_id` int(11) NOT NULL DEFAULT 0,
  `flight_days` text DEFAULT NULL,
  `from_airport` varchar(3) DEFAULT NULL,
  `to_airport` varchar(3) DEFAULT NULL,
  `departure_time` int(11) DEFAULT NULL,
  `arrival_time` int(11) DEFAULT NULL,
  `airline_flight` text DEFAULT NULL,
  `airline_code` varchar(3) DEFAULT NULL,
  `flight_number` int(11) DEFAULT NULL,
  `aircraft_code_sequence` text DEFAULT NULL,
  `meal_code` text DEFAULT NULL,
  `stops` int(11) DEFAULT NULL,
  `connections` int(11) DEFAULT NULL,
  `dual_carrier` text DEFAULT NULL,
  `time_elapsed` int(11) DEFAULT NULL,
  PRIMARY KEY (`flight_id`),
  KEY `flight_aircraft_code_sequence` (`aircraft_code_sequence`(100)),
  KEY `flight_dual_carrier` (`dual_carrier`(100)),
  KEY `flight_flight_days` (`flight_days`(100)),
  KEY `flight_airline_flight` (`airline_flight`(100)),
  KEY `flight_airline_code` (`airline_code`),
  KEY `flight_meal_code` (`meal_code`(100)),
  KEY `departure_time` (`departure_time`),
  KEY `arrival_time` (`arrival_time`),
  KEY `flight_number` (`flight_number`),
  KEY `stops` (`stops`),
  KEY `connections` (`connections`),
  KEY `time_elapsed` (`time_elapsed`),
  KEY `airline_code` (`airline_code`),
  KEY `from_airport` (`from_airport`),
  KEY `to_airport` (`to_airport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `flight_fare`
--

DROP TABLE IF EXISTS `flight_fare`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `flight_fare` (
  `flight_id` int(11) DEFAULT NULL,
  `fare_id` int(11) DEFAULT NULL,
  KEY `flight_id` (`flight_id`),
  KEY `fare_id` (`fare_id`),
  KEY `flight_id_2` (`flight_id`,`fare_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `flight_leg`
--

DROP TABLE IF EXISTS `flight_leg`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `flight_leg` (
  `flight_id` int(11) DEFAULT NULL,
  `leg_number` int(11) DEFAULT NULL,
  `leg_flight` int(11) DEFAULT NULL,
  KEY `flight_id` (`flight_id`),
  KEY `leg_number` (`leg_number`),
  KEY `leg_flight` (`leg_flight`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `flight_stop`
--

DROP TABLE IF EXISTS `flight_stop`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `flight_stop` (
  `flight_id` int(11) DEFAULT NULL,
  `stop_number` int(11) DEFAULT NULL,
  `stop_days` text DEFAULT NULL,
  `stop_airport` text DEFAULT NULL,
  `arrival_time` int(11) DEFAULT NULL,
  `arrival_airline` text DEFAULT NULL,
  `arrival_flight_number` int(11) DEFAULT NULL,
  `departure_time` int(11) DEFAULT NULL,
  `departure_airline` text DEFAULT NULL,
  `departure_flight_number` int(11) DEFAULT NULL,
  `stop_time` int(11) DEFAULT NULL,
  KEY `flight_stop_departure_airline` (`departure_airline`(100)),
  KEY `flight_stop_stop_days` (`stop_days`(100)),
  KEY `flight_stop_stop_airport` (`stop_airport`(100)),
  KEY `flight_stop_arrival_airline` (`arrival_airline`(100)),
  KEY `flight_id` (`flight_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `food_service`
--

DROP TABLE IF EXISTS `food_service`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `food_service` (
  `meal_code` text DEFAULT NULL,
  `meal_number` int(11) DEFAULT NULL,
  `compartment` text DEFAULT NULL,
  `meal_description` varchar(10) DEFAULT NULL,
  KEY `food_service_meal_code` (`meal_code`(100)),
  KEY `food_service_compartment` (`compartment`(100)),
  KEY `meal_number` (`meal_number`),
  KEY `meal_description` (`meal_description`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ground_service`
--

DROP TABLE IF EXISTS `ground_service`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ground_service` (
  `city_code` text DEFAULT NULL,
  `airport_code` text DEFAULT NULL,
  `transport_type` text DEFAULT NULL,
  `ground_fare` int(11) DEFAULT NULL,
  KEY `ground_service_airport_code` (`airport_code`(100)),
  KEY `ground_service_transport_type` (`transport_type`(100)),
  KEY `ground_service_city_code` (`city_code`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `month`
--

DROP TABLE IF EXISTS `month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `month` (
  `month_number` int(11) DEFAULT NULL,
  `month_name` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `restriction`
--

DROP TABLE IF EXISTS `restriction`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `restriction` (
  `restriction_code` text DEFAULT NULL,
  `advance_purchase` int(11) DEFAULT NULL,
  `stopovers` text DEFAULT NULL,
  `saturday_stay_required` text DEFAULT NULL,
  `minimum_stay` int(11) DEFAULT NULL,
  `maximum_stay` int(11) DEFAULT NULL,
  `application` text DEFAULT NULL,
  `no_discounts` text DEFAULT NULL,
  KEY `restriction_stopovers` (`stopovers`(100)),
  KEY `restriction_restriction_code` (`restriction_code`(100)),
  KEY `restriction_application` (`application`(100)),
  KEY `restriction_saturday_stay_required` (`saturday_stay_required`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `state`
--

DROP TABLE IF EXISTS `state`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `state` (
  `state_code` text DEFAULT NULL,
  `state_name` text DEFAULT NULL,
  `country_name` text DEFAULT NULL,
  KEY `state_state_code` (`state_code`(100)),
  KEY `state_state_name` (`state_name`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `time_interval`
--

DROP TABLE IF EXISTS `time_interval`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `time_interval` (
  `period` text DEFAULT NULL,
  `begin_time` int(11) DEFAULT NULL,
  `end_time` int(11) DEFAULT NULL,
  KEY `time_interval_period` (`period`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `time_zone`
--

DROP TABLE IF EXISTS `time_zone`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `time_zone` (
  `time_zone_code` text DEFAULT NULL,
  `time_zone_name` text DEFAULT NULL,
  `hours_from_gmt` int(11) DEFAULT NULL,
  KEY `time_zone_time_zone_name` (`time_zone_name`(100)),
  KEY `time_zone_time_zone_code` (`time_zone_code`(100))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'atis'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-01-02 10:34:32
