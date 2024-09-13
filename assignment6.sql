create or replace database my_par_database;

 use schema public;

  create or replace table cities (
    continent varchar default null,
    country varchar default null,
    city variant default null
  );

create or replace warehouse my_par_warehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

use warehouse my_par_warehouse;


CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = 'parquet';

CREATE OR REPLACE STAGE sf_tut_stage
FILE_FORMAT = sf_tut_parquet_format;
 

PUT file://C:\temp\load\cities.parquet @sf_tut_stage;
PUT file:///workspaces/Snowflake_Task/cities.parquet.gz @sf_tut_stage;



list @my_par_database.public.sf_tut_stage;


copy into cities
 from (select $1:continent::varchar,
              $1:country:name::varchar,
              $1:country:city::variant
      from @sf_tut_stage/cities.parquet)
      FILE_FORMAT = (TYPE = 'PARQUET');
      
COPY INTO cities
FROM @sf_tut_stage/cities.parquet.gz
FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'GZIP')
ON_ERROR = 'CONTINUE';

use database my_par_database;

COPY INTO cities
FROM @sf_tut_stage/cities.parquet
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'CONTINUE';


copy into @sf_tut_stage/out/parquet_
from (select continent,
             country,
             c.value::string as city
     from cities,
          lateral flatten(input => city) c)
  file_format = (type = 'parquet')
  header = true;

