-- Q1
-- Create Role as per mentioned hierarchy
CREATE ROLE admin;
CREATE ROLE developer;
CREATE ROLE pii;

GRANT ROLE admin TO ROLE accountadmin;
GRANT ROLE developer TO ROLE admin;
GRANT ROLE pii TO ROLE accountadmin;

-- Q2
-- Create an M-sized warehouse using the accountadmin role
-- name -> assignment_wh
CREATE WAREHOUSE assignment_wh
WITH 
WAREHOUSE_SIZE = 'MEDIUM'
AUTO_SUSPEND = 900
MAX_CLUSTER_COUNT = 5
ENABLE_QUERY_ACCELERATION = TRUE
COMMENT = 'Warehouse for assignment';

USE WAREHOUSE assignment_wh;

-- Q3
-- Switch to Admin Role
USE ROLE admin;

-- Q4
-- Create a database assignment_db
USE ROLE accountadmin;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE admin WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;

USE ROLE admin;
CREATE DATABASE assignment_db;

-- Q5
-- Create a schema my_schema
USE ROLE accountadmin;
GRANT CREATE SCHEMA ON DATABASE assignment_db TO ROLE admin;

USE ROLE admin;
CREATE SCHEMA assignment_db.my_schema;

-- Q6 - 9
-- Part 1
-- Create a table using sample csv
-- Load data from internal stage
CREATE STAGE local_upload_csv;
-- Had to comment below line to let other lines run
-- PUT file:///Users/dvsingla/mydox/snowflake/employees.csv @assignment_db.my_schema.local_upload_csv;
LIST @local_upload_csv;

CREATE FILE FORMAT employee_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
COMMENT = 'The csv has header and some values are enclosed by "(ones which contain ",")';

CREATE TABLE employees (
    organization_id        VARCHAR(30),
    name                   VARCHAR(50),
    website                VARCHAR(50),
    country                VARCHAR(100),
    description            VARCHAR(300),
    founded                INTEGER,
    industry               VARCHAR(300),
    number_of_employees    INTEGER,
    elt_ts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by                 VARCHAR(30) DEFAULT 'LOCAL',
    file_name              VARCHAR(30)
);

COPY INTO employees (organization_id, name, website, country, description, founded, industry, number_of_employees, file_name)
FROM 
(SELECT $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME FROM @local_upload_csv)
FILES = ('employees.csv.gz')
FILE_FORMAT = (FORMAT_NAME = 'employee_csv_format')
ON_ERROR = ABORT_STATEMENT;

SELECT TOP 100 * FROM employees;

-- Part 2
-- Create a variant dataset
-- Use external stage for loading
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ADMIN;

USE ROLE ADMIN;

CREATE OR REPLACE STORAGE INTEGRATION aws_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::797722239421:role/snowflake-user'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-assgnmt/');

DESC INTEGRATION aws_integration;

CREATE OR REPLACE STAGE external_aws_upload
  STORAGE_INTEGRATION = aws_integration
  URL = 's3://snowflake-assgnmt/';

LIST @external_aws_upload;
DESC STAGE external_aws_upload;
  
CREATE TABLE employees_external (
    organization_id        VARCHAR(30),
    name                   VARCHAR(50),
    website                VARCHAR(50),
    country                VARCHAR(100),
    description            VARCHAR(300),
    founded                INTEGER,
    industry               VARCHAR(300),
    number_of_employees    INTEGER,
    elt_ts                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by                 VARCHAR(30) DEFAULT 'LOCAL',
    file_name              VARCHAR(30)
);

COPY INTO employees_external (organization_id, name, website, country, description, founded, industry, number_of_employees, file_name)
FROM
(SELECT  $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME FROM @external_aws_upload)
FILES = ('employees.csv')
FILE_FORMAT = (FORMAT_NAME = 'employee_csv_format')
ON_ERROR = ABORT_STATEMENT;

SELECT TOP 100 * FROM employees_external;

CREATE OR REPLACE TABLE employees_external_variant(col VARIANT) AS (
SELECT PARSE_JSON('{
    "organization_id": "' || $2 || '",
    "name": "' || $3 || '",
    "website": "' || $4 || '",
    "country": "' || $5 || '",
    "description": "' || $6 || '",
    "founded" : "' || $7 || '",
    "industry" : "' || $8 || '",
    "number_of_employees" : "' || $9 || '",
    "elt_ts" : "' || CURRENT_TIMESTAMP() || '",
    "elt_by" : "aws",
    "file_name" : "' || METADATA$FILENAME || '"
  }')
  FROM @external_aws_upload/employees.csv ( FILE_FORMAT => 'employee_csv_format')
);

SELECT * FROM employees_external_variant;

-- Q10
-- Upload parquet file to stage and infer schema
LIST @external_aws_upload;

CREATE FILE FORMAT parquet_format
TYPE = parquet;

SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@external_aws_upload', FILE_FORMAT => 'parquet_format', FILES => 'cities.parquet'));

-- Q11
-- Run select on parquet file from stage
SELECT * FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');
SELECT $1:continent FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');
SELECT $1:country:city FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');

-- Q12
-- Add masking policy to hide details from developer role but not from PII role
CREATE OR REPLACE MASKING POLICY mask_country
 AS (val VARCHAR) RETURNS
 VARCHAR ->
     CASE
         WHEN CURRENT_ROLE() IN ('DEVELOPER') THEN '**masked**'
         ELSE val
     END;

ALTER TABLE IF EXISTS employees MODIFY COLUMN country SET MASKING POLICY mask_country;
ALTER TABLE IF EXISTS employees_external MODIFY COLUMN country SET MASKING POLICY mask_country;

GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE developer;
GRANT USAGE ON DATABASE assignment_db TO ROLE developer;
GRANT USAGE ON SCHEMA my_schema TO ROLE developer;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA my_schema TO ROLE developer;

GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE pii;
GRANT USAGE ON DATABASE assignment_db TO ROLE pii;
GRANT USAGE ON SCHEMA my_schema TO ROLE pii;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA my_schema TO ROLE pii;

USE ROLE developer;
SELECT * FROM assignment_db.my_schema.employees;

USE ROLE pii;
SELECT * FROM assignment_db.my_schema.employees;
