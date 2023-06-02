

Rajkeshav review
He had written a  SQL language that is used to perform given queries in Snowflake. Below is the explanation of each query -

 ## Role Management:

Roles (admin, developer, PII) are created using the CREATE ROLE statement.
The admin role is granted to the accountadmin role.
The developer role is granted to the admin role.
The PII role is granted to the accountadmin role.

 ## Warehouse Creation:
The assignment_wh warehouse is created using the CREATE WAREHOUSE statement.
The warehouse size, type, and other configurations are specified.

 ## Database and Schema Creation:
The assignment_db database is created using the CREATE DATABASE statement.
The my_schema schema is created within the assignment_db database using the CREATE SCHEMA statement.

 ## Table Creation:
The employee table is created with columns first_name, last_name, email, phone, gender, department, job_title, years_of_experience, and salary.
The in_employee table is created with similar columns, but with the first_name column defined as VARCHAR(255).

 ## Staging and Data Loading:

The internal_stage stage is created using the CREATE STAGE statement.
The my_csv_format file format is created for CSV files using the CREATE FILE FORMAT statement.
Data from the CSV file employees.csv.gz is loaded into the in_employee table using the COPY INTO statement and the my_csv_format file format.
The external_stage stage is created for loading data from an external source.
A storage integration (s3_integration) is created for accessing data from an S3 bucket.
Data from the S3 bucket is loaded into the employee table using the COPY INTO statement and the s3_integration storage integration.

 ## Masking Policies:

Masking policies (hideEmail_mask and hidePhone_mask) are created to hide sensitive information in the in_employee table.
The ALTER TABLE statement is used to apply the masking policies to the email and phone columns.

 ## Role-Based Privileges:

Privileges are granted to the developer role on the assignment_wh warehouse, ASSIGNMENT_DB database, MY_SCHEMA schema, and in_employee table.
Privileges are granted to the PII role on the same objects as the developer role.
