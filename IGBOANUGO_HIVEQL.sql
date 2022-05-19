-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/clinicaltrial')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/clinicaltrial_2021.csv', '/FileStore/tables/clinicaltrial')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables/clinicaltrial')

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS clinictable(
Id string, 
Sponsor string,
Status string,
Start string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string)
row format delimited  
fields terminated by '|'
location '/FileStore/tables/clinicaltrial'

-- COMMAND ----------

select * from clinictable limit 8

-- COMMAND ----------

SELECT COUNT (*) as count  FROM clinictable

-- COMMAND ----------

SELECT TYPE, COUNT(TYPE) as type_count 
FROM clinictable
GROUP BY TYPE
ORDER BY COUNT(TYPE) DESC

-- COMMAND ----------

SELECT AILMENT,COUNT(*)as Frequencies
FROM (SELECT EXPLODE(SPLIT(CONDITIONS, ',') ) AS AILMENT
      FROM clinictable)
WHERE AILMENT != ''
GROUP BY AILMENT
ORDER BY COUNT(*) DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/meshcsv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/mesh.csv', '/FileStore/tables/meshcsv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables/meshcsv')

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS meshtable(
term string,
tree string)
row format delimited  
fields terminated by ','
location '/FileStore/tables/meshcsv'

-- COMMAND ----------

SELECT REPLACE(SUBSTRING(TREE, 1, 3), '"', ' ') AS TREE, COUNT(AILMENT) AS AILMENT_COUNT
FROM meshtable
INNER JOIN (SELECT EXPLODE(SPLIT(CONDITIONS, ',') ) AS AILMENT
      FROM clinictable)
ON TERM = AILMENT
WHERE AILMENT != ''
GROUP BY REPLACE(SUBSTRING(TREE, 1, 3), '"', ' ')
ORDER BY COUNT(AILMENT) DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/FileStore/tables/pharmacsv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('/FileStore/tables/pharma.csv', '/FileStore/tables/pharmacsv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables/pharmacsv')

-- COMMAND ----------


CREATE EXTERNAL TABLE IF NOT EXISTS pharmatable(
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string,
Penalty_Date string,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string,
Action_Type string,
Agency string,
Civil_Criminal string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string,
City string,
Address string,
Zip string,
NAICS_Code string,
NAICS_Translation string,
HQ_Country_of_Parent string, 
HQ_State_of_Parent string,
Ownership_Structure string,
Parent_Company_Stock_Ticker string,
Major_Industry_of_Parent string,
Specific_Industry_of_Parent string,
Info_Source string,
Notes string)
row format delimited
fields terminated by ','
location '/FileStore/tables/pharmacsv'

-- COMMAND ----------

select sponsor AS SPONSORS, count(id) as ID_COUNT
from clinictable
left join pharmatable
on replace(parent_company, '"', "")= sponsor
where regexp_replace (parent_company, '"', "") is null 
group by sponsor 
order by count(id) desc
limit 10


-- COMMAND ----------

select substring(completion, 1, 3),count (substring(completion, 1, 3))
from clinictable
where status= 'Completed' and substring(completion, 5, 4)=2021
group by substring(completion, 1, 3)

-- COMMAND ----------

select substring(completion, 1, 3),count (substring(completion, 1, 3))
from clinictable
where status= 'Completed' and substring(completion, 5, 4)=2021
group by substring(completion, 1, 3)

-- COMMAND ----------


