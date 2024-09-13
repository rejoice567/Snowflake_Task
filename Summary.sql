USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

select o_custkey

from ORDERS

group by o_custkey;


select o_custkey,

sum(o_totalprice) as total_sales,

count(*) as number_of_orders

 from orders
 
 group by o_custkey;

 select count(*) as num_orders,
 min(o_totalprice) as min_price,
 max(o_totalprice) as max_price,
 avg(o_totalprice) as avg_price
 from orders;

 select date_part(year, o_orderdate) as order_year,
 count(*) as num_orders,
 min(o_totalprice) as min_price,
 max(o_totalprice) as max_price,
 avg(o_totalprice) as avg_price
 from orders
 group by date_part(year, o_orderdate);

 select count(*) as total_orders,

 count(distinct o_custkey) as num_customers,

 count(distinct date_part(year, o_orderdate)) as num_years

 from orders;


select date_part(year, o_orderdate) as year,
 min(o_orderdate) as first_order,
 max(o_orderdate) as last_order,
 avg(o_totalprice) as avg_price,
 sum(o_totalprice) as tot_sales
 from orders
 group by date_part(year, o_orderdate);


select r.r_name,

listagg(n.n_name,',') 

within group (order by n.n_name) as nation_list

from region r inner join nation n

on r.r_regionkey = n.n_regionkey

group by r.r_name; 

USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;
select n.n_name, c.c_mktsegment, count(*)

from customer c inner join nation n

on c.c_nationkey = n.n_nationkey

where n.n_regionkey = 1

group by n.n_name, c.c_mktsegment

order by 1,2;


select date_part(year, o.o_orderdate) as year,
 datediff(month, o.o_orderdate, 
 l.l_shipdate) as months_to_ship,
 count(*)
 from orders o inner join lineitem l
 on o.o_orderkey = l.l_orderkey
 where o.o_orderdate >= '01-JAN-1997'::date
 group by date_part(year, o.o_orderdate),
 datediff(month, o.o_orderdate, l.l_shipdate)
 order by 1,2;

 select date_part(year, o.o_orderdate) as year,
 datediff(month, o.o_orderdate,
 l.l_shipdate) as months_to_ship,
 count(*)
 from orders o inner join lineitem l
 on o.o_orderkey = l.l_orderkey
 where o.o_orderdate >= '01-JAN-1997'::date
 group by all
 order by 1,2;

 select n.n_name, c.c_mktsegment, count(*)
 from customer c inner join nation n
 on c.c_nationkey = n.n_nationkey
 where n.n_regionkey = 1
 group by rollup(n.n_name, c.c_mktsegment)
 order by 1,2;

USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;
 select o_custkey, sum(o_totalprice)
 from orders
 where 1998 = date_part(year, o_orderdate)
 group by o_custkey
 order by 1;