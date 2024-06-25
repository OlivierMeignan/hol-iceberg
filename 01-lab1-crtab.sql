 -- Iceberg tables creation
-- From Impala Virtual Warehouse
DROP DATABASE IF EXISTS ${userdb} cascade;
CREATE DATABASE ${userdb};

USE ${userdb};

CREATE  TABLE ${userdb}.flights_iceberg 
PARTITIONED BY (year) 
STORED AS ICEBERG AS 
SELECT * FROM ${dbcsv}.flights_csv
WHERE year <=2006;


CREATE  TABLE ${userdb}.planes_iceberg
STORED AS ICEBERG AS
SELECT * FROM ${dbcsv}.planes_csv;

CREATE  TABLE ${userdb}.airlines_iceberg
STORED AS ICEBERG AS
SELECT * FROM ${dbcsv}.airlines_csv;

CREATE EXTERNAL TABLE ${userdb}.airports_parquet
STORED AS PARQUET AS
SELECT * FROM ${dbcsv}.airports_csv;

CREATE  TABLE ${userdb}.unique_tickets_iceberg
STORED AS ICEBERG AS
SELECT * FROM ${dbcsv}.unique_tickets_csv;


COMPUTE STATS ${userdb}.flights_iceberg;
COMPUTE STATS ${userdb}.planes_iceberg;
COMPUTE STATS ${userdb}.airlines_iceberg;
COMPUTE STATS ${userdb}.airports_parquet;
COMPUTE STATS ${userdb}.unique_tickets_iceberg;



