USE ROLE accountadmin;

USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;


CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);


SELECT * FROM tasty_bytes_sample_data.raw_pos.menu;

CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu;


SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;






