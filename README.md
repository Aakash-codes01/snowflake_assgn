# snowflake_assgn
# Snowflake Assgn

## Building Roles, Databases and Schemas
- At first three roles are created: admin, developer, pii.
- Then developer role was granted to admin role which was further granted to accountadmin, pii was separately granted to accountadmin to build the specified hierarchy.
- Then a `MEDIUM` sized warehouse was created named `assignment_wh`. To give `admin` access to the warehouse, `GRANT USAGE` is used by accountadmin. Also, the `WITH GRANT OPTION` is mentioned to allow `admin` to grant the same privelege to other roles.
- Then to create database and schema, the respective priveleges of `CREATE DATABASE` and `CREATE SCHEMA` are granted to admin via accountadmin. Assignment_db database and schema named my_schema are created by admin.

```sql
CREATE ROLE admin;
CREATE ROLE developer;
CREATE ROLE pii;

GRANT ROLE admin TO ROLE accountadmin;
GRANT ROLE developer TO ROLE admin;
GRANT ROLE pii TO ROLE accountadmin;

CREATE WAREHOUSE assignment_wh
WITH 
WAREHOUSE_SIZE = 'MEDIUM'
AUTO_SUSPEND = 900
MAX_CLUSTER_COUNT = 5
ENABLE_QUERY_ACCELERATION = TRUE
COMMENT = 'Warehouse for assignment';

USE WAREHOUSE assignment_wh;
USE ROLE admin;
USE ROLE accountadmin;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE admin WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;

USE ROLE admin;
CREATE DATABASE assignment_db;

USE ROLE accountadmin;
GRANT CREATE SCHEMA ON DATABASE assignment_db TO ROLE admin;

USE ROLE admin;
CREATE SCHEMA assignment_db.my_schema;
```

## Creating Stages and Tables
### Via Internal Staging
- Created named internal stage - `local_upload_csv`. Uploaded a csv file(*employees.csv*) to the stage using `PUT`.
- Then made a file format(`employee_csv_format`) for the csv with `FIELD_OPTIONALLY_ENCLOSED_BY` set to `"`. This tells that some values might have comma in them but to not treat the comma as field separator if the value is surrounded by double quotes.
- Then created a table - `employees` and unloaded the csv file from the stage into it using `COPY INTO ... FROM (SELECT ...)`. Three other columns - `elt_ts` set to `DEFAULT CURRENT TIMESTAMP`, `elt_by` set to `DEFAULT 'LOCAL'` and `file_name` set to `METADATA$FILENAME` were also added while unloading.



```sql
SELECT TOP 100 * FROM employees;
```
<img src = 'https://github.com/ds-cr/snowflakeAssgn/blob/main/photos/employees.png' alt = 'employees'>

```sql
CREATE STAGE local_upload_csv;

PUT file:///Users/aks/mydox/snowflake/employees.csv @assignment_db.my_schema.local_upload_csv;
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
```

### Via External Staging
- First granted `CREATE INTEGRATION` to admin via accountadmin.
- Then created storage integration named `aws_integration` and assigned it to a stage `aws_external_upload`.
- Then a table `employees_external` is created in exactly the same way as in the internal stage (first table is created and the data is unloaded using same parameters)
- To create a **variant** version another table `employees_external_variant` is created. This is done via `CREATE TABLE employee_external_variant(col VARIANT) AS (SELECT ...)` command; along with that `PARSE_JSON` is used to parse and convert the stage data to a variant format.

```sql
SELECT TOP 100 * FROM employee_external_variant;
```
<img src = 'https://github.com/ds-cr/snowflakeAssgn/blob/main/photos/employees variant.png' alt = 'employees variant' width=550>

```sql
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ADMIN;

USE ROLE ADMIN;

CREATE OR REPLACE STORAGE INTEGRATION aws_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::797722239421:role/snowflake_role_dhruv'
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
```

## Working with parquet
- First a file format `parquet_format` was created for the subsequent queries.
- The schema of parquet file is inferred using `INFER_SCHEMA`
- Then different nested objects were queried using the `$` syntax like `$1:continent` and `$1:country:city` in the `SELECT` query.

Infer parquet schema <br>
<img src = 'https://github.com/ds-cr/snowflakeAssgn/blob/main/photos/parquet infer.png' alt = 'infer query on parquet' width=750>

Select query on parquet via stage <br>
<img src = 'https://github.com/ds-cr/snowflakeAssgn/blob/main/photos/parquet select.png' alt = 'select query on parquet' width=650>

```sql
LIST @external_aws_upload;

CREATE FILE FORMAT parquet_format
TYPE = parquet;

SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@external_aws_upload', FILE_FORMAT => 'parquet_format', FILES => 'cities.parquet'));

-- Q11
-- Run select on parquet file from stage
SELECT * FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');
SELECT $1:continent FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');
SELECT $1:country:city FROM @external_aws_upload(PATTERN => '.*\.parquet', FILE_FORMAT => 'parquet_format');
```

## Masking
- A masking policy `mask_country` was created for the `country` column. The `employees` and `employees_external` were subsequently altered and the masking policy was set.
- Then permissions on warehouse, schema and tables were granted to developer and pii roles. `SELECT` was used to observe the masking effect.

Select via developer
<img src = 'https://github.com/ds-cr/snowflakeAssgn/blob/main/photos/masking.png' alt = 'masking'>

```sql
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
```
