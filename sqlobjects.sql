Create or replace database sf_tuts;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

create or replace table emp_basic (
first_name STRING,
last_name STRING,
email STRING,
streetaddress STRING,
city STRING,
start_date DATE);


create or replace WAREHOUSE sf_tuts_wh WITH
WAREHOUSE_SIZE='X-SMALL'
AUTO_SUSPEND = 180
AUTO_RESUME =TRUE
INITIALLY_SUSPENDED =TRUE;

SELECT CURRENT_WAREHOUSE();


---uploading the all files to stage
PUT file:///workspaces/Snowflake_Task/data/employees0*.csv @sf_tuts.public.%emp_basic;

---check available files in stage
list @sf_tuts.public.%emp_basic;

 
copy into emp_basic
FROM @sf_tuts.public.%emp_basic
FILE_FORMAT=(type='csv' field_optionally_enclosed_by='"')
PATTERN = '.*0[1-5].csv.gz'
ON_ERROR = 'skip_file';

SELECT * FROM emp_basic;

INSERT INTO emp_basic VALUES
   ('Clementine','Adamou','cadamou@sf_tuts.com','10510 Sachs Road','Klenak','2017-9-22') ,
   ('Marlowe','De Anesy','madamouc@sf_tuts.co.uk','36768 Northfield Plaza','Fangshan','2017-1-26');


   SELECT email FROM emp_basic WHERE email LIKE '%.uk';

   SELECT first_name, last_name, DATEADD('day',90,start_date) FROM emp_basic WHERE start_date <= '2017-01-01';