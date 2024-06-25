


-- Create airlines csv tables

-- From Hive Virtual Warehouse
-- Create csv database
-- DROP DATABASE IF EXISTS ${dbcsv} CASCADE;
CREATE DATABASE ${dbcsv};

-- Create csv table flights_csv
--
DROP TABLE IF EXISTS ${dbcsv}.flights_csv;
CREATE EXTERNAL TABLE ${dbcsv}.flights_csv(month int, dayofmonth int, dayofweek int, deptime int, crsdeptime
int, arrtime int, crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, depdelay int, origin string, dest string,
distance int, taxiin int, taxiout int, cancelled int, cancellationcode string, diverted string,
carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int, year int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${storage}/data/airlines-csv/flights'
tblproperties("skip.header.line.count"="1");


-- Should be 86289323 rows
SELECT COUNT (*) FROM ${dbcsv}.flights_csv;


-- Create csv table planes_csv
--
DROP TABLE IF EXISTS ${dbcsv}.planes_csv;
CREATE EXTERNAL TABLE ${dbcsv}.planes_csv(tailnum string, owner_type string, manufacturer string, issue_date
string, model string, status string, aircraft_type string, engine_type string, year int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${storage}/data/airlines-csv/planes'
tblproperties("skip.header.line.count"="1");

-- Should be 5029 rows
SELECT COUNT (*) FROM ${dbcsv}.planes_csv;


-- Create csv table airlines_csv
--
DROP TABLE IF EXISTS ${dbcsv}.airlines_csv;
CREATE EXTERNAL TABLE ${dbcsv}.airlines_csv(code string, description string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${storage}/data/airlines-csv/airlines/'
tblproperties("skip.header.line.count"="1");


-- Should be 1491 rows
SELECT COUNT (*) FROM ${dbcsv}.airlines_csv;

-- Create csv table airports_csv
--
DROP TABLE IF EXISTS ${dbcsv}.airports_csv;
CREATE EXTERNAL TABLE ${dbcsv}.airports_csv(iata string, airport string, city string, state string, country
string, lat DOUBLE, lon DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${storage}/data/airlines-csv/airports'
tblproperties("skip.header.line.count"="1");


-- Should be 3376 rows
SELECT COUNT (*) FROM ${dbcsv}.airports_csv;


-- Create csv table unique_tickets_csv
--
DROP TABLE IF EXISTS ${dbcsv}.unique_tickets_csv;
CREATE external TABLE ${dbcsv}.unique_tickets_csv (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier
STRING, leg1origin STRING, leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,
leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,
leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,
leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT, leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime
BIGINT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${storage}/data/airlines-csv/unique_tickets'
tblproperties("skip.header.line.count"="1");


-- Should be 100000 rows
SELECT COUNT (*) FROM ${dbcsv}.unique_tickets_csv;



