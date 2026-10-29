CREATE DATABASE IF NOT EXISTS bdct;
USE bdct;

CREATE TABLE IF NOT EXISTS user_data (
  data STRING,
  user_id STRING,
  properties STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE user_data VALUES
('record1','U1','Age=21;state=CA;gender=M'),
('record2','U2','Age=25;state=TX;gender=F'),
('record3','U3','Age=21;state=CA;gender=F'),
('record4','U4','Age=30;state=NY;gender=M');

SELECT * FROM user_data;

SELECT
  MIN(CAST(REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1) AS INT)) AS min_age,
  MAX(CAST(REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1) AS INT)) AS max_age,
  COUNT(DISTINCT REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1)) AS unique_ages
FROM user_data;

SELECT
  REGEXP_EXTRACT(properties, 'state=([^;]+)', 1) AS state,
  COUNT(*) AS total
FROM user_data
GROUP BY REGEXP_EXTRACT(properties, 'state=([^;]+)', 1);
