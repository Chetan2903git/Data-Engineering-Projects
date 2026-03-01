use Olist_Ecommerce;

## Part B: Data Manipulation & Retrieval (5 questions)
##6. Insert cleaned data using transactions - demonstrate COMMIT and ROLLBACK with error scenarios

##7. Find customers who have the same email but different names/addresses (data quality red flag)
select * from customers
where customer_email in(
	select customer_email
    from customers
    group by customer_email
    having count(distinct customer_name)>1 or count( distinct customer_address)>1
    )
    order by customer_email;

## as name and address is not present in data we can check for same customer_unique_id and different city and state
SELECT customer_unique_id,
       COUNT(DISTINCT customer_city) AS city_count,
       COUNT(DISTINCT customer_state) AS state_count
FROM customers
GROUP BY customer_unique_id
HAVING city_count > 1
   OR state_count > 1;

##8. Identify orphan records: order_items that reference non-existent products or orders
select * 
from order_items
where product_id is null;

##9. List customers who registered but never placed an order (conversion funnel leak analysis)
select customer_unique_id as Customer_never_placed_order
from customers c
left join orders o on c.customer_id=o.customer_id
where o.order_id is null;

##10. Detect potential fraud: orders where payment_value != order total (sum of order_items)
with total_order_value as 
(select order_id,round(sum(price + freight_value),0) as total_value
from order_items
group by order_id
),
total_payment as
(select order_id, round(sum(payment_value),0) as total_paid
from payments
group by order_id
),
final as (
select p.order_id, p.total_paid,o.total_value,
case when p.total_paid!=o.total_value then 1 else 0 end as fraud_identifier
from total_payment p
join total_order_value o 
on o.order_id=p.order_id)

Select * from final;

## Part C: Complex Aggregations (5 questions)
##11. Calculate monthly revenue with Month-over-Month (MoM) growth percentage - handle first month edge case
with monthly_revenue as (
	select sum(p.payment_value) as total_revenue, 
    date_format(o.order_purchase_timestamp, '%Y-%m') as order_month
	from orders o
    join payments p
    on o.order_id = p.order_id
    where o.order_status='delivered'
    group by date_format(o.order_purchase_timestamp, '%Y-%m')
)
select order_month, total_revenue,
		lag(total_revenue) over(order by order_month) as previous_month_revenue,
        case when lag(total_revenue) over(order by order_month) is null then null
			 else round((total_revenue - lag(total_revenue) over(order by order_month))/lag(total_revenue) over(order by order_month)*100, 2)
		end as Monthly_growth_percentage
from monthly_revenue
order by order_month;

##12. Find top 10 products by revenue in each category using RANK() or DENSE_RANK()
with product_revenue as(
	select pr.product_id,pr.product_category_name,sum(p.payment_value) as item_revenue
    from payments p
    join order_items ot on p.order_id=ot.order_id
    join products pr on ot.product_id= pr.product_id
    group by pr.product_id,pr.product_category_name
)
select * 
from (select product_category_name, 
			 product_id,item_revenue,
			 rank() over(partition by  product_category_name order by item_revenue desc) as product_rank
			 from product_revenue) ranked
where product_rank<=10;                      

##13. Calculate Customer Lifetime Value (CLV) = SUM(payment_value) per customer, categorize as Bronze/Silver/Gold
with customer_revenue as(
	select c.customer_unique_id, sum(p.payment_value) as customer_revenue
    from customers c
    join orders o on c.customer_id=o.customer_id
    join payments p on o.order_id=p.order_id
    group by c.customer_unique_id
)
select customer_unique_id, customer_revenue as customer_lifetime_value, 
	   case when customer_revenue between 0 and 2500 then 'Bronze'
			when customer_revenue between 2500 and 5000 then 'Silver'
            when customer_revenue>5000 then 'Gold'
            end as category
from customer_revenue
order by customer_revenue desc;

##14. Create a sales report with daily, weekly, monthly subtotals using ROLLUP or CUBE
select 
    IFNULL(sale_month, 'ALL MONTHS') as sale_month,
    IFNULL(sale_week, 'ALL WEEKS') as sale_week,
    IFNULL(sale_date, 'ALL DAYS') as sale_date,
    SUM(payment_value) as total_sales
from (
    select 
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') as sale_month,
        CONCAT(year(o.order_purchase_timestamp),'-W',week(o.order_purchase_timestamp, 1)) as sale_week,
        date(o.order_purchase_timestamp) as sale_date,
        p.payment_value
    from orders o
    join payments p 
        on o.order_id = p.order_id
    where o.order_status = 'delivered'
) t
group by sale_month, sale_week, sale_date
with rollup;

##15. Identify seasonal patterns: which product categories sell best in which months?
WITH category_monthly_sales AS (
    SELECT 
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS sale_month,
        pr.product_category_name,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM orders o
    JOIN order_items oi 
        ON o.order_id = oi.order_id
    JOIN products pr 
        ON oi.product_id = pr.product_id
    WHERE o.order_status = 'delivered'
    GROUP BY sale_month, pr.product_category_name
)

SELECT *
FROM (
    SELECT 
        sale_month,
        product_category_name,
        total_revenue,
        DENSE_RANK() OVER (
            PARTITION BY sale_month
            ORDER BY total_revenue DESC
        ) AS category_rank
    FROM category_monthly_sales
) ranked
WHERE category_rank = 1
ORDER BY sale_month;

## Part D: Mastering Joins (5 questions)
##16. Create a complete customer 360-degree view joining 5+ tables (customers, orders, order_items, products, reviews)alter
select c.customer_unique_id, o.*, ot.order_item_id, p.product_category_name, r.review_id
from orders o
left join customers c on o.customer_id=c.customer_id
left join order_items ot on o.order_id=ot.order_id
left join products p on p.product_id=ot.product_id
left join order_reviews r on r.order_id=o.order_id;

##17. Find customers who bought products from category 'electronics' but never from 'books'
select distinct(c.customer_unique_id)
from orders o
join customers c on o.customer_id=c.customer_id
join order_items ot on o.order_id=ot.order_id
join products p on p.product_id=ot.product_id
join product_categories pg on pg.product_category_name=p.product_category_name
where pg.product_category_name_english like ('%electronics%')
and not exists (
    select 1
    from orders o2
    join order_items ot2 on o2.order_id = ot2.order_id
    join products p2 on p2.product_id = ot2.product_id
    join product_categories pg2 on pg2.product_category_name = p2.product_category_name
    where o2.customer_id = o.customer_id
      and pg2.product_category_name_english like '%book%'
);

select distinct(c.customer_unique_id)
from orders o
join customers c on o.customer_id=c.customer_id
join order_items ot on o.order_id=ot.order_id
join products p on p.product_id=ot.product_id
join product_categories pg on pg.product_category_name=p.product_category_name
where pg.product_category_name_english like ('%book%');

##18. List sellers and their best-selling product in each category they sell
select seller_id, product_category_name, product_id, total_sales
from (
		select oi.seller_id,p.product_category_name,oi.product_id,
				count(*) as total_sales, row_number() over( partition by oi.seller_id,p.product_category_name order by count(*) desc) as rn
		from order_items oi
        join products p on oi.product_id=p.product_id
        group by oi.seller_id, p.product_category_name, oi.product_id
        ) t
where rn=1;

##19. Identify products frequently bought together (market basket analysis using self-join)


##20. Find orders with shipping delays: expected_delivery_date < actual_delivery_date, show customer and seller info
select o.order_id, c.customer_id, oi.seller_id, datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date) as delay_days
from orders o
join customers c on  o.customer_id=c.customer_id
join order_items oi on oi.order_id=o.order_id
where  o.order_status = 'delivered' 
and o.order_delivered_customer_date IS NOT NULL 
and o.order_estimated_delivery_date < o.order_delivered_customer_date
order by delay_days desc;

## Part E: Subqueries & CTEs (4 questions)
##21. Find customers who spent more than the average customer in their state (correlated subquery)
select * from (
select c.customer_state,c.customer_unique_id, sum(p.payment_value) as total_spend
from customers c
join orders o on o.customer_id=c.customer_id
join payments p on o.order_id=p.order_id
group by c.customer_state,c.customer_unique_id
)t 
where total_spend > 
(select avg(state_total) 
	from ( select c2.customer_unique_id, sum(p2.payment_value) as state_total
			from customers c2
            join orders o2 on o2.customer_id=c2.customer_id
            join payments p2 on o2.order_id=p2.order_id
            where c2.customer_state=t.customer_state
            group by c2.customer_unique_id
)s
);

##22. Get the 2nd highest revenue-generating product in each category (ranking with subquery)
select * from (
select product_category_name, product_id, product_revenue,
rank() over( partition by product_category_name order by product_revenue desc) as product_rank from
(select pr.product_category_name, pr.product_id, sum(p.payment_value) as product_revenue
from products pr
join order_items o on pr.product_id=o.product_id
join payments p on p.order_id=o.order_id
group by pr.product_category_name, pr.product_id
) t
) a where product_rank=2;

##23. Using recursive CTE, create a product category hierarchy (if categories have parent-child relationships)
#parent chilc relationship does not exist here

##24. Find customers who made purchases in 3+ consecutive months using CTE with window functions
with monthly_order as (
select distinct c.customer_unique_id, date_format(o.order_purchase_timestamp, '%Y-%m-01') as order_month
from orders o
join customers c on c.customer_id=o.customer_id
where order_status='delivered'
),
numbered as (
select customer_unique_id, order_month, row_number() over( partition by customer_unique_id order by order_month) as rn
from monthly_order
),
grouped as (
select customer_unique_id, order_month, date_sub(order_month, interval rn month) as grp
from numbered
)
select customer_unique_id, count(*) as number_of_consecative_months
from grouped
group by customer_unique_id, grp
having count(*)>=3
order by count(*) desc;

## Part F: Advanced Window Functions (4 questions)
##25. Calculate 7-day moving average of daily order count
with daily_count as (
select date(order_purchase_timestamp) as order_date, count(*) as daily_orders
from orders o
where order_status='delivered'
group by date(order_purchase_timestamp)
)
select order_date, daily_orders, 
avg(daily_orders) over(order by order_date rows between 6 preceding and current row) as Moving_average_7_days
from daily_count
order by order_date;

##26. For each customer, find the gap (in days) between consecutive orders using LAG()
with ordered as(
select c.customer_unique_id, date(o.order_purchase_timestamp) as order_date,
lag(date(o.order_purchase_timestamp)) over( partition by c.customer_unique_id order by o.order_purchase_timestamp) as previous_order_date
from orders o
join customers c on c.customer_id=o.customer_id
where o.order_status='delivered'
)
select customer_unique_id, order_date, previous_order_date, datediff(order_date,previous_order_date) as gap_in_days
from ordered
order by gap_in_days desc;

##27. Rank sellers by revenue within each state, show only top 3 per state
with revenue_per_seller_state as (
select s.seller_state, o.seller_id, sum(payment_value) as revenue_per_seller
from order_items o
join sellers s on o.seller_id=s.seller_id
join payments p on p.order_id=o.order_id
group by s.seller_state, o.seller_id
order by s.seller_state
)
select* from (
select seller_state, seller_id, revenue_per_seller, dense_rank() over (partition by seller_state order by revenue_per_seller desc) as top_3_sellers
from revenue_per_seller_state
) t
where top_3_sellers<=3;


##28. Calculate running total of revenue by date with percentage of grand total
with daily_revenue as(
select date(o.order_purchase_timestamp) as order_date, sum(p.payment_value) as daily_revenue
from orders o
join payments p on o.order_id=p.order_id
where order_status='delivered'
group by date(o.order_purchase_timestamp)
)
select order_date, daily_revenue,
sum(daily_revenue) over(order by order_date rows between unbounded preceding and current row) as running_total,
round( sum(daily_revenue) over(order by order_date rows between unbounded preceding and current row)/sum(daily_revenue) over()*100,2) as percent_of_total
from daily_revenue
order by order_date;

##29. Create a stored procedure to calculate dynamic discount based on rules:
## New customer (first order): 15% off
## Order value > $500: 10% off
## Loyal customer (5+ previous orders): 5% off
## Apply maximum one discount per order

DROP PROCEDURE calculate_discount;

DELIMITER //

CREATE PROCEDURE calculate_discount (
    IN p_order_id VARCHAR(50)
)
BEGIN
    DECLARE v_customer_unique_id VARCHAR(50);
    DECLARE v_order_value DECIMAL(10,2);
    DECLARE v_previous_orders INT;
    DECLARE v_discount_percent DECIMAL(5,2);

    -- Get customer_unique_id for this order
    SELECT c.customer_unique_id
    INTO v_customer_unique_id
    FROM orders o
    JOIN customers c 
        ON o.customer_id = c.customer_id
    WHERE o.order_id = p_order_id;

    -- Get total order value
    SELECT SUM(payment_value)
    INTO v_order_value
    FROM payments
    WHERE order_id = p_order_id;

    -- Count previous delivered orders using customer_unique_id
    SELECT COUNT(*)
    INTO v_previous_orders
    FROM orders o
    JOIN customers c 
        ON o.customer_id = c.customer_id
    WHERE c.customer_unique_id = v_customer_unique_id
      AND o.order_status = 'delivered';

    -- Determine max discount
    SET v_discount_percent = GREATEST(
        CASE WHEN v_previous_orders = 0 THEN 15 ELSE 0 END,
        CASE WHEN v_order_value > 500 THEN 10 ELSE 0 END,
        CASE WHEN v_previous_orders >= 5 THEN 5 ELSE 0 END
    );

    -- Return result
    SELECT 
        p_order_id AS order_id,
        v_customer_unique_id AS customer_unique_id,
        v_order_value AS order_value,
        v_previous_orders AS previous_orders,
        v_discount_percent AS discount_percent,
        (v_order_value * v_discount_percent / 100) AS discount_amount,
        (v_order_value - (v_order_value * v_discount_percent / 100)) AS final_amount;

END //

DELIMITER ;

CALL calculate_discount('b850a16d8faf65a74c51285ef34379ce');