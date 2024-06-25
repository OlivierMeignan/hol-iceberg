-- 
-- 1) Convert tables
-- Compare table properties. Notice : EXTERNAL, SCHEMA DEFINITION

DESCRIBE FORMATTED ${userdb}.flights_iceberg;

DESCRIBE FORMATTED ${userdb}.airports_parquet;

ALTER TABLE ${userdb}.airports_parquet CONVERT TO ICEBERG;


ALTER TABLE ${userdb}.airports_parquet RENAME TO ${userdb}.airports_iceberg;


DESCRIBE FORMATTED ${userdb}.airports_iceberg;


--
--  2 ) Alter partitions

--  S3 partitions=1/1 files=1 size=128.60MB
-- row-size=8B cardinality=714.19K
EXPLAIN SELECT year, month, count(*) FROM ${userdb}.flights_iceberg
WHERE year = 2006 AND month = 12
GROUP BY year, month;


ALTER TABLE ${userdb}.flights_iceberg SET PARTITION spec ( year, month );


--  S3 partitions=1/1 files=1 size=128.60MB
EXPLAIN SELECT year, month, count(*) FROM ${userdb}.flights_iceberg
WHERE year = 2006 AND month = 12
GROUP BY year, month;


INSERT INTO ${userdb}.flights_iceberg
SELECT * FROM ${dbcsv}.flights_csv WHERE year = 2007;

--  S3 partitions=1/1 files=1 size=128.60MB
EXPLAIN SELECT year, month, count(*) FROM ${userdb}.flights_iceberg
WHERE year = 2006 AND month = 12
GROUP BY year, month;

--  S3 partitions=1/1 files=1 size=11.19MB
EXPLAIN SELECT year, month, count(*) FROM ${userdb}.flights_iceberg
WHERE year = 2007 AND month = 12
GROUP BY year, month;


--
-- 3) Snapshots

INSERT INTO ${userdb}.flights_iceberg
SELECT * FROM ${dbcsv}.flights_csv
WHERE year >= 2008;


DESCRIBE HISTORY ${userdb}.flights_iceberg;

--
-- 3) Time travel

-- SELECT DATA USING TIMESTAMP FOR SNAPSHOT
SELECT year, count(*)
FROM ${userdb}.flights_iceberg
FOR SYSTEM_TIME AS OF '${create_ts}'
GROUP BY year
ORDER BY year desc;


 -- SELECT DATA USING ID FOR SNAPSHOT
SELECT year, count(*)
FROM ${userdb}.flights_iceberg
FOR SYSTEM_VERSION AS OF ${snapshot_id}
GROUP BY year
ORDER BY year desc;


-- 4) Rollback

ALTER TABLE ${userdb}.flights_iceberg EXECUTE rollback(${snapshot_id});
-- or  
-- ALTER TABLE user001db.flights_iceberg EXECUTE roll- back(${snapshot_id});

SELECT year, count(*)
FROM ${userdb}.flights_iceberg
GROUP BY year
ORDER BY year desc;

DESCRIBE HISTORY ${userdb}.flights_iceberg;



-- 5) Schema evolution
-- ALTER TABLE ... RENAME TO ... (renames the table if the Iceberg catalog supports it)
-- ALTER TABLE ... CHANGE COLUMN ... (change name and type of a column iff the new type is compatible with the old type)
-- ALTER TABLE ... ADD COLUMNS ... (adds columns to the end of the table)
-- ALTER TABLE ... DROP COLUMN ...


DESCRIBE FORMATTED ${userdb}.airlines_iceberg;

ALTER TABLE ${userdb}.airlines_iceberg 
   ADD COLUMNS(status STRING, updated TIMESTAMP);

DESCRIBE FORMATTED ${userdb}.airlines_iceberg;

INSERT INTO ${userdb}.airlines_iceberg
      VALUES("Z111","Adrenaline Airways","NEW",from_unixtime(unix_timestamp()));

SELECT * FROM ${userdb}.airlines_iceberg WHERE code > "Z" ORDER BY code;


DESCRIBE FORMATTED ${userdb}.airlines_iceberg;



-- 6) Update (Impala 4.4+)
--


UPDATE ${userdb}.airlines_iceberg SET code = 'Z112' WHERE code = 'Z111';

ALTER TABLE ${userdb}.airlines_iceberg SET TBLPROPERTIES('format-version'='2');

UPDATE ${userdb}.airlines_iceberg SET code = 'Z112' WHERE code = 'Z111';

SELECT * FROM ${userdb}.airlines_iceberg WHERE code > "Z11" ORDER BY code;




-- Branching/tagging in HIVE / CDE Cloud 
-- 7) Historical Tags
-- https://iceberg.apache.org/docs/latest/branching/#historical-tags
-- Tags can be used for retaining  historical snapshots
-- Tags are immutable labels for individual snapshots

-- Create a tag for the end of the year and retain it forever
DESCRIBE HISTORY ${userdb}.flights_iceberg;

-- Create a tag for the end of the year and retain it forever
-- ALTER TABLE ${userdb}.flights_iceberg 
-- CREATE TAG 'EOY-2007' FOR SYSTEM_VERSION AS OF ${snapshot_id};

-- SELECT * FROM ${userdb}.flights_iceberg VERSION AS OF 'EOY-2007';
