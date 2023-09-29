--dedup_weather
drop table if exists dedup_weather;

create table dedup_weather as
select
	created_at :: date as "date",
	created_at,
	country,
	city,
	weather,
	main,
	wind,
	clouds,
	sys
from
	(
	select
		*,
		row_number() over (
                partition by country,
		city
	order by
		created_at desc
            ) as rn
	from
		stg_weather
    ) p
where
	rn = 1;


--merged_sales
drop table if exists merged_sales;

create table merged_sales as
select
	*
from
	stg_sales p
left join stg_users q on
	p.customer_id = q.id
left join dedup_weather r on
	p.store_city = r.city
	and p.store_country = r.country
	and p.order_date = r."date" :: date
order by
	order_date;


--aggr_customer
drop table if exists aggr_customer;

create table aggr_customer as
select
	customer_id,
	sum(quantity * price) as total_sales
from
	merged_sales
group by
	customer_id
order by
	total_sales desc;


--aggr_product
drop table if exists aggr_product;

create table aggr_product as
select
	product_id,
	round(avg(quantity),
	2) as avg_order_quantity,
	sum(quantity * price) as total_sales
from
	merged_sales
group by
	product_id
order by
	total_sales desc;


--aggr_month
drop table if exists aggr_month;

create table aggr_month as 
select
	date_trunc('month',
	order_date)::date as "month",
	sum(quantity * price) as total_sales
from
	merged_sales
group by
	"month"
order by
	"month";


--aggr_quarter
drop table if exists aggr_quarter;

select
	date_trunc('quarter',
	order_date)::date as "quarter",
	sum(quantity * price) as total_sales
from
	merged_sales
group by
	"quarter"
order by
	"quarter";