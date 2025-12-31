-- MySQL dump 10.13  Distrib 8.0.44, for Linux (x86_64)
--
-- Host: localhost    Database: advising
-- ------------------------------------------------------
-- Server version	8.0.44

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `AREA`
--

DROP TABLE IF EXISTS `AREA`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `AREA` (
  `course_id` int DEFAULT NULL,
  `area` varchar(30) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `COMMENT_INSTRUCTOR`
--

DROP TABLE IF EXISTS `COMMENT_INSTRUCTOR`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COMMENT_INSTRUCTOR` (
  `instructor_id` int NOT NULL DEFAULT '0',
  `student_id` int NOT NULL DEFAULT '0',
  `score` int DEFAULT NULL,
  `comment_text` varchar(400) DEFAULT NULL,
  PRIMARY KEY (`instructor_id`,`student_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `COURSE`
--

DROP TABLE IF EXISTS `COURSE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COURSE` (
  `COURSE_ID` int NOT NULL DEFAULT '0',
  `NAME` varchar(255) DEFAULT NULL,
  `DEPARTMENT` varchar(255) DEFAULT NULL,
  `NUMBER` varchar(255) DEFAULT NULL,
  `CREDITS` varchar(255) DEFAULT NULL,
  `ADVISORY_REQUIREMENT` varchar(255) DEFAULT NULL,
  `ENFORCED_REQUIREMENT` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(16384) DEFAULT NULL,
  `NUM_SEMESTERS` int DEFAULT NULL,
  `NUM_ENROLLED` int DEFAULT NULL,
  `HAS_DISCUSSION` varchar(1) DEFAULT NULL,
  `HAS_LAB` varchar(1) DEFAULT NULL,
  `HAS_PROJECTS` varchar(1) DEFAULT NULL,
  `HAS_EXAMS` varchar(1) DEFAULT NULL,
  `NUM_REVIEWS` int DEFAULT NULL,
  `CLARITY_SCORE` int DEFAULT NULL,
  `EASINESS_SCORE` int DEFAULT NULL,
  `HELPFULNESS_SCORE` int DEFAULT NULL,
  PRIMARY KEY (`COURSE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `COURSE_OFFERING`
--

DROP TABLE IF EXISTS `COURSE_OFFERING`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COURSE_OFFERING` (
  `OFFERING_ID` int NOT NULL DEFAULT '0',
  `COURSE_ID` int DEFAULT NULL,
  `SEMESTER` int DEFAULT NULL,
  `SECTION_NUMBER` int DEFAULT NULL,
  `START_TIME` time DEFAULT NULL,
  `END_TIME` time DEFAULT NULL,
  `MONDAY` varchar(1) DEFAULT NULL,
  `TUESDAY` varchar(1) DEFAULT NULL,
  `WEDNESDAY` varchar(1) DEFAULT NULL,
  `THURSDAY` varchar(1) DEFAULT NULL,
  `FRIDAY` varchar(1) DEFAULT NULL,
  `SATURDAY` varchar(1) DEFAULT NULL,
  `SUNDAY` varchar(1) DEFAULT NULL,
  `HAS_FINAL_PROJECT` varchar(1) DEFAULT 'N',
  `HAS_FINAL_EXAM` varchar(1) DEFAULT 'N',
  `TEXTBOOK` varchar(30) DEFAULT NULL,
  `CLASS_ADDRESS` varchar(30) DEFAULT NULL,
  `ALLOW_AUDIT` varchar(1) DEFAULT 'N',
  PRIMARY KEY (`OFFERING_ID`),
  KEY `COURSE_ID` (`COURSE_ID`),
  CONSTRAINT `COURSE_OFFERING_ibfk_1` FOREIGN KEY (`COURSE_ID`) REFERENCES `COURSE` (`COURSE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `COURSE_PREREQUISITE`
--

DROP TABLE IF EXISTS `COURSE_PREREQUISITE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COURSE_PREREQUISITE` (
  `pre_course_id` int NOT NULL,
  `course_id` int NOT NULL,
  PRIMARY KEY (`course_id`,`pre_course_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `COURSE_TAGS_COUNT`
--

DROP TABLE IF EXISTS `COURSE_TAGS_COUNT`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `COURSE_TAGS_COUNT` (
  `COURSE_ID` int NOT NULL DEFAULT '0',
  `CLEAR_GRADING` int DEFAULT '0',
  `POP_QUIZ` int DEFAULT '0',
  `GROUP_PROJECTS` int DEFAULT '0',
  `INSPIRATIONAL` int DEFAULT '0',
  `LONG_LECTURES` int DEFAULT '0',
  `EXTRA_CREDIT` int DEFAULT '0',
  `FEW_TESTS` int DEFAULT '0',
  `GOOD_FEEDBACK` int DEFAULT '0',
  `TOUGH_TESTS` int DEFAULT '0',
  `HEAVY_PAPERS` int DEFAULT '0',
  `CARES_FOR_STUDENTS` int DEFAULT '0',
  `HEAVY_ASSIGNMENTS` int DEFAULT '0',
  `RESPECTED` int DEFAULT '0',
  `PARTICIPATION` int DEFAULT '0',
  `HEAVY_READING` int DEFAULT '0',
  `TOUGH_GRADER` int DEFAULT '0',
  `HILARIOUS` int DEFAULT '0',
  `WOULD_TAKE_AGAIN` int DEFAULT '0',
  `GOOD_LECTURE` int DEFAULT '0',
  `NO_SKIP` int DEFAULT '0',
  PRIMARY KEY (`COURSE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `GSI`
--

DROP TABLE IF EXISTS `GSI`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `GSI` (
  `course_offering_id` int NOT NULL DEFAULT '0',
  `student_id` int NOT NULL,
  PRIMARY KEY (`course_offering_id`,`student_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `INSTRUCTOR`
--

DROP TABLE IF EXISTS `INSTRUCTOR`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `INSTRUCTOR` (
  `INSTRUCTOR_ID` int NOT NULL DEFAULT '0',
  `NAME` varchar(255) DEFAULT NULL,
  `UNIQNAME` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`INSTRUCTOR_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `OFFERING_INSTRUCTOR`
--

DROP TABLE IF EXISTS `OFFERING_INSTRUCTOR`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `OFFERING_INSTRUCTOR` (
  `OFFERING_INSTRUCTOR_ID` int NOT NULL DEFAULT '0',
  `OFFERING_ID` int DEFAULT NULL,
  `INSTRUCTOR_ID` int DEFAULT NULL,
  PRIMARY KEY (`OFFERING_INSTRUCTOR_ID`),
  KEY `OFFERING_ID` (`OFFERING_ID`),
  KEY `INSTRUCTOR_ID` (`INSTRUCTOR_ID`),
  CONSTRAINT `OFFERING_INSTRUCTOR_ibfk_1` FOREIGN KEY (`OFFERING_ID`) REFERENCES `COURSE_OFFERING` (`OFFERING_ID`),
  CONSTRAINT `OFFERING_INSTRUCTOR_ibfk_2` FOREIGN KEY (`INSTRUCTOR_ID`) REFERENCES `INSTRUCTOR` (`INSTRUCTOR_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `PROGRAM`
--

DROP TABLE IF EXISTS `PROGRAM`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `PROGRAM` (
  `program_id` int NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `college` varchar(255) DEFAULT NULL,
  `introduction` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`program_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `PROGRAM_COURSE`
--

DROP TABLE IF EXISTS `PROGRAM_COURSE`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `PROGRAM_COURSE` (
  `program_id` int NOT NULL DEFAULT '0',
  `course_id` int NOT NULL DEFAULT '0',
  `workload` int DEFAULT NULL,
  `category` varchar(11) NOT NULL DEFAULT '',
  PRIMARY KEY (`program_id`,`course_id`,`category`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `PROGRAM_REQUIREMENT`
--

DROP TABLE IF EXISTS `PROGRAM_REQUIREMENT`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `PROGRAM_REQUIREMENT` (
  `program_id` int NOT NULL,
  `category` varchar(11) NOT NULL,
  `min_credit` int DEFAULT NULL,
  `additional_req` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`program_id`,`category`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `SEMESTER`
--

DROP TABLE IF EXISTS `SEMESTER`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `SEMESTER` (
  `semester_id` int NOT NULL,
  `semester` varchar(4) DEFAULT NULL,
  `year` int DEFAULT NULL,
  PRIMARY KEY (`semester_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `STUDENT`
--

DROP TABLE IF EXISTS `STUDENT`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `STUDENT` (
  `student_id` int NOT NULL,
  `lastname` varchar(255) DEFAULT NULL,
  `firstname` varchar(255) DEFAULT NULL,
  `program_id` int DEFAULT NULL,
  `declare_major` varchar(255) DEFAULT NULL,
  `total_credit` int DEFAULT NULL,
  `total_gpa` float(3,2) DEFAULT NULL,
  `entered_as` varchar(11) DEFAULT 'FirstYear',
  `admit_term` int DEFAULT NULL,
  `predicted_graduation_semester` int DEFAULT NULL,
  `degree` varchar(10) DEFAULT NULL,
  `minor` varchar(10) DEFAULT NULL,
  `internship` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`student_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `STUDENT_RECORD`
--

DROP TABLE IF EXISTS `STUDENT_RECORD`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `STUDENT_RECORD` (
  `student_id` int NOT NULL,
  `course_id` int NOT NULL,
  `semester` int NOT NULL,
  `grade` varchar(10) DEFAULT NULL,
  `how` varchar(10) DEFAULT NULL,
  `transfer_source` varchar(10) DEFAULT NULL,
  `earn_credit` varchar(1) NOT NULL DEFAULT 'Y',
  `repeat_term` varchar(10) DEFAULT NULL,
  `test_id` varchar(10) DEFAULT NULL,
  `offering_id` int DEFAULT NULL,
  PRIMARY KEY (`student_id`,`course_id`,`earn_credit`),
  KEY `course_id` (`course_id`),
  CONSTRAINT `STUDENT_RECORD_ibfk_1` FOREIGN KEY (`student_id`) REFERENCES `STUDENT` (`student_id`),
  CONSTRAINT `STUDENT_RECORD_ibfk_2` FOREIGN KEY (`course_id`) REFERENCES `COURSE` (`COURSE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'advising'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-12-30 23:34:00
