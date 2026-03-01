use Olist_Ecommerce;

##Detect and remove Duplicate customers.

##check customer table if duplicate records exist
select customer_id, count(*)
from customers
group by customer_id
having count(*)>1
order by count(*) desc;

##update orders table if duplicate records exist
select order_id, count(*)
from orders
group by order_id
having count(*)>1
order by count(*) desc;

##check product table if duplicate records exist
select product_id, count(*)
from products
group by product_id
having count(*)>1
order by count(*) desc;

##check sellers table if duplicate records exist
select seller_id, count(*)
from sellers
group by seller_id
having count(*)>1
order by count(*) desc;

##check geolocation table if duplicate records exist
SELECT geolocation_zip_code_prefix,
       geolocation_lat,
       geolocation_lng,
       geolocation_city,
       geolocation_state,
       COUNT(*)
FROM geolocation
GROUP BY geolocation_zip_code_prefix,
         geolocation_lat,
         geolocation_lng,
         geolocation_city,
         geolocation_state
HAVING COUNT(*) > 1;

## duplicate dosent exist in any table ( if any table has duplicate we can do it using below query)
with geo_duplicate as( select * , row_number() over(partition by 
							geolocation_zip_code_prefix,
                            geolocation_lat,
                            geolocation_lng,
                            geolocation_city,
                            geolocation_state
	order by geolocation_zip_code_prefix ) as rn
    from geolocation
)
delete g
from geolocation g
join geo_duplicate d on g.geolocation_zip_code_prefix = d.geolocation_zip_code_prefix
						AND g.geolocation_lat = d.geolocation_lat
						AND g.geolocation_lng = d.geolocation_lng
						AND g.geolocation_city = d.geolocation_city
						AND g.geolocation_state = d.geolocation_state
where d.rn>1;



