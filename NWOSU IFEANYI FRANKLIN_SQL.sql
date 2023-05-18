-- Databricks notebook source
-- MAGIC %md
-- MAGIC In this part, Spark SQL was used to convert “Clinicaltrial” DataFrame that was created in DF to SQL tables and views so as to run SQL scripts on the tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC VIEWING THE CONTENT OF FILE STORE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fileroot = 'clinicaltrial_2021'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #CONVERTING TO A DATAFRAME
-- MAGIC clinicDF = spark.read.options(delimiter=",").csv("/FileStore/tables/" + fileroot + '.csv', header =True)
-- MAGIC clinicDF.show(truncate = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #CONVERTING TO A DATAFRAME
-- MAGIC from pyspark.sql.types import *
-- MAGIC myRDD1 = sc.textFile("/FileStore/tables/" + fileroot + '.csv') \
-- MAGIC            .map(lambda line: line.split("|")) \
-- MAGIC            .filter(lambda row: row[0] != "Id")  # filter out the header row
-- MAGIC            
-- MAGIC clinicDF1 = spark.createDataFrame(myRDD1, schema=StructType([
-- MAGIC     StructField("Id", StringType()),
-- MAGIC     StructField("Sponsor", StringType()),
-- MAGIC     StructField("Status", StringType()),
-- MAGIC     StructField("Start", StringType()),
-- MAGIC     StructField("Completion", StringType()),
-- MAGIC     StructField("Type", StringType()),
-- MAGIC     StructField("Submissions", StringType()),
-- MAGIC     StructField("Conditions", StringType()),
-- MAGIC     StructField("Interventions", StringType())
-- MAGIC ]))
-- MAGIC clinicDF1.show(5, truncate=False)
-- MAGIC clinicDF1.printSchema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating SQL Databases and Tables,Firstly,a temporary table (view) was created from the clinic DataFrame , queries were on this view. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #CREATING A VIEW IN THE PYTHON CELL USING THE CLINIC DATAFRAME
-- MAGIC clinicDF1.createOrReplaceTempView("clinicSQL")

-- COMMAND ----------

--CHECKING THE CONTENTS OF THE VIEW
SELECT * FROM clinicSQL LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #NUMBER OF STUDIES : QUESTION ONE PROCEDURE AND ANSWER

-- COMMAND ----------

--USING SELECT AND COUNTING THE OBSERVATIONS IN THE CLINICSQL VIEW
SELECT COUNT(*) as STUDY_COUNT FROM clinicSQL



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TYPES OF STUDIES AND FREQUENCY:  Q2 ANSWER

-- COMMAND ----------

--HERE THE TYPE COLUMN WAS COUNTED FOR FREQUENCY OF OCCURENCE AND ORDERED BY THE FREQUENCY IN A DESCENDING MANNER
SELECT Type, COUNT(*) as Frequency
FROM clinicSQL
GROUP BY Type
ORDER BY frequency DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TOP 5 CONDITIONS: QUESTION THREE PROCEDURE AND ANSWER

-- COMMAND ----------

--HERE THE CONDITIONS COLUMN WAS COUNTED FOR FREQUENCY OF OCCURENCE AND ORDERED BY THE FREQUENCY IN A DESCENDING MANNER
SELECT Conditions, COUNT(*) as Frequency
FROM clinicSQL
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5

-- COMMAND ----------

--this query first splits the Conditions column using the comma separator, and then explodes the resulting array into separate rows. The resulting words are then trimmed of any extra whitespace before being grouped and counted.
--REMOVING THE EMPTY STRINGS IN COLUMN AND FURTHER SPLITTING OF THE VALUES UNDER THE COLUMN
SELECT word AS Conditions, COUNT(*) AS Frequency
FROM (
  SELECT TRIM(word) AS word
  FROM (
    SELECT EXPLODE(SPLIT(Conditions, ',')) AS word
    FROM clinicSQL
    WHERE Conditions != ''
  ) AS split_words
) AS words
GROUP BY word
ORDER BY Frequency DESC
LIMIT 5


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TOP 10 COMMON SPONSORS: QUESTION FOUR PROCEDURE AND ANSWER

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #TO MAKE IT RERUNNABLE A VARIABLE WAS DECLARED TO HOLD THE FILE
-- MAGIC fileroot1 = 'pharma'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #reading the pharma file into a dataframe
-- MAGIC pharmDF = spark.read.options(delimiter=",").csv("/FileStore/tables/" + fileroot1 + '.csv', header =True)
-- MAGIC pharmDF.show(5,truncate = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #CREATING A VIEW IN THE PYTHON CELL USING THE PHARM DATAFRAME
-- MAGIC pharmDF.createOrReplaceTempView("pharmSQL")

-- COMMAND ----------

--CHECKING THE CONTENTS OF THE VIEW
SELECT * FROM pharmSQL LIMIT 10

-- COMMAND ----------

create or replace table default.pharm1 as select * from pharmSQL

-- COMMAND ----------

create database if not exists PharmCom1

-- COMMAND ----------

create table if not exists PharmCom1.pharm1 as select * from default.pharm1

-- COMMAND ----------

create or replace temp view splitp as select split(Parent_Company,',') as split_p, Parent_Company from PharmCom1.pharm1

-- COMMAND ----------

select * from splitp limit 10

-- COMMAND ----------

create or replace temp view explodedph as select split_p, explode(split_p) from splitp

-- COMMAND ----------

select * from explodedph limit 10

-- COMMAND ----------

--this query first splits the Conditions column using the comma separator, and then explodes the resulting array into separate rows. The resulting words are then trimmed of any extra whitespace before being grouped and counted.
--REMOVING THE EMPTY STRINGS IN COLUMN AND FURTHER SPLITTING OF THE VALUES UNDER THE COLUMN
SELECT word AS Parent_Company, COUNT(*) AS Frequency
FROM (
  SELECT TRIM(word) AS word
  FROM (
    SELECT EXPLODE(SPLIT(Parent_Company, ",")) AS word
    FROM pharmSQL
    WHERE Parent_Company != ''
  ) AS split_words
) AS words
GROUP BY word
ORDER BY Frequency DESC
LIMIT 10

-- COMMAND ----------

--SORTING OUT THE VALUES FROM SPONSOR COLUMN OF THE CLINICAL TRIAL
SELECT Sponsor, COUNT(*) AS Frequency
FROM clinicSQL
WHERE Sponsor != ''
GROUP BY Sponsor
ORDER BY Frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #question 4 DF answer

-- COMMAND ----------

--Question 4 Answer 10 most common non-pharmaceutical companies
--filtering the uncommon column in pharmSQL from SponsorSQL to get the non pharmaceutical companies

-- COMMAND ----------

--CREATING TEMPORAL VIEWS, TABLES AND DATABASES TO HELP IN QUERYING

create or replace table default.sponsor as select * from clinicSQL


-- COMMAND ----------

create database if not exists Spons

-- COMMAND ----------

create table if not exists Spons.sponsor2 as select * from default.sponsor

-- COMMAND ----------

create or replace temp view subsponsor as select Sponsor from Spons.sponsor2

-- COMMAND ----------

select * from subsponsor

-- COMMAND ----------

--creating temporal view for the pharm
create or replace temp view subpharm as select Parent_Company from PharmCom1.pharm1

-- COMMAND ----------

select * from subpharm

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #QUESTION 4 FINAL ANSWER

-- COMMAND ----------

--FILTERING THE TWO VIEWS TO DISPLAY THE NON PHARMACEUTICAL COMPANIES
SELECT Sponsor, COUNT(*) as Frequency
FROM subsponsor
WHERE Sponsor NOT IN (
    SELECT Parent_Company FROM subpharm
)
GROUP BY Sponsor
ORDER BY Frequency DESC
LIMIT 10


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #NUMBER OF COMPLETED STUDIES EACH MONTH : QUESTION 5

-- COMMAND ----------

--CREATING VIEWS FOR THE STATUS AND COMPLETION COLUMNS TO ENABLE QUERYING
create or replace temp view substatus as select Status from Spons.sponsor2

-- COMMAND ----------

select * from substatus 

-- COMMAND ----------

create or replace temp view subcomplete as select Completion from Spons.sponsor2

-- COMMAND ----------

select * from subcomplete

-- COMMAND ----------

create or replace temp view subcplt as select 'Completed' from substatus



-- COMMAND ----------

select * from subcplt

-- COMMAND ----------

--CHECKING OUT
CREATE OR REPLACE TEMP VIEW merged AS
SELECT substatus.*, subcomplete.* FROM substatus, subcomplete;

-- COMMAND ----------

select * from merged

-- COMMAND ----------

SELECT *
FROM merged
WHERE Status = 'Completed' AND Completion LIKE '%2021'



-- COMMAND ----------

--SORTING THE MONTHS FOR 2021 AND COUNTING THE FREQUENCY OF OCCURENCE
SELECT Completion, COUNT(completion) AS Frequency
FROM clinicSQL
WHERE Status = 'Completed' AND Completion LIKE '%2021%'
GROUP BY Completion;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC This query below converts the Completion string to a date format using the to_date() function, then extracts the month and year from the date using the date_format() function and formats it as MMM yyyy. It then groups the results by the extracted month and year, counts the frequency of each Completion, and sorts the results by the month and year in ascending order using ORDER BY clause

-- COMMAND ----------

--DISPLAYING THE RESULT STARTING WITH JANUARY
SELECT date_format(to_date(Completion, 'MMM yyyy'), 'MMM yyyy') as Month, COUNT(completion) as Frequency
FROM clinicSQL
WHERE Status = 'Completed' AND Completion LIKE '%2021%'
GROUP BY Month
ORDER BY to_date(concat('01 ', Month), 'dd MMM yyyy');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #ADDITIONAL QUESTIONS - OPTIONAL PART FOR EXTRA GRADE

-- COMMAND ----------

--QUESTION 1
--TO KNOW HOW MANY SPONSORS WHO ARE ALSO A UNIVERSITY AND NOT PHARMACEUTICAL COMPANY
Select * from clinicSQL
where CHARINDEX('University', Sponsor) > 0

--The query above returns all Sponsors who has 'University' in thier name the CHARINDEX  was used to search for UNIVERSITY in the Sponsor

-- COMMAND ----------

--THIS QUERY COUNTS THE NUMBER OF UNIVERSITY SPONSORS
SELECT COUNT(*) FROM clinicSQL
WHERE CHARINDEX('University', Sponsor) > 0;


-- COMMAND ----------

--QUESTION 2
-- THIS QUERY FINDS OUT FROM THE CLINICAL TRIALS, CASES WITH 'UNKNOWN' STATUS OR CONDITIONS WITH 'HEART FAILURE' SO AS TO EXPEDIATE ACTIONS
--IT WILL LATER COUNT NUMBER OF CONDITIONS RESULTING FROM HEART FAILURES
SELECT * FROM clinicSQL
WHERE Status LIKE '%unkown status%' OR Conditions LIKE '%Heart Failure%';

-- COMMAND ----------

--HERE THE NUMBER OF CONDITIONS RESULTING FROM HEART FAILURES WAS COMPUTED FOR PROPER DOCUMENTATION
SELECT COUNT(Conditions) FROM clinicSQL
WHERE Conditions LIKE '%Heart Failure%';

-- COMMAND ----------

--QUESTION 3
--TO COUNT THE TOTAL OF SUBMISSIONS MADE IN A PARTICULAR MONTH AND YEAR
-- HERE SUBMISSIONS MADE IN MARCH 2021 WAS COUNTED AND DISPLAYED

SELECT COUNT(*) AS Total_Submissions
FROM clinicSQL
WHERE to_date(Submissions, 'MMM yyyy') = to_date('Mar 2021', 'MMM yyyy');


-- COMMAND ----------

--ANOTHER METHOD FOR Q3
SELECT COUNT(*) AS Total_Submissions
FROM clinicSQL
WHERE Submissions like '%Mar%' AND Submissions like '%2021%';


