use Olist_Ecommerce;
##30. Identify slowest queries using EXPLAIN ANALYZE, optimize with appropriate indexes (B-tree, Hash), show before/after execution time


##1. Q.21 Find customers who spent more than the average customer in their state (correlated subquery)
EXPLAIN ANALYZE
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
) limit 200;

## the above analyze auery was failing due to timeout now we will add index for optimization

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_customers_state ON customers(customer_state);
CREATE INDEX idx_customers_id ON customers(customer_id);

## now we are able to run the query

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
)limit 200;


##2. Q28 Calculate running total of revenue by date with percentage of grand total
explain analyze with daily_revenue as(
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

#after optimization-
##'-> Sort: daily_revenue.order_date  (actual time=249..249 rows=611 loops=1)\n    -> Table scan on <temporary>  (cost=2.5..2.5 rows=0) (actual time=249..249 rows=611 loops=1)\n        -> Temporary table  (cost=0..0 rows=0) (actual time=249..249 rows=611 loops=1)\n            -> Window aggregate with buffering: sum(daily_revenue.daily_revenue) OVER ()   (actual time=248..249 rows=611 loops=1)\n                -> Table scan on <temporary>  (cost=2.5..2.5 rows=0) (actual time=248..248 rows=611 loops=1)\n                    -> Temporary table  (cost=0..0 rows=0) (actual time=248..248 rows=611 loops=1)\n                        -> Window aggregate: sum(daily_revenue.daily_revenue) OVER (ORDER BY daily_revenue.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   (actual time=247..248 rows=611 loops=1)\n                            -> Table scan on <temporary>  (cost=2.5..2.5 rows=0) (actual time=247..247 rows=611 loops=1)\n                                -> Temporary table  (cost=0..0 rows=0) (actual time=247..247 rows=611 loops=1)\n                                    -> Window aggregate: sum(daily_revenue.daily_revenue) OVER (ORDER BY daily_revenue.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)   (actual time=247..247 rows=611 loops=1)\n                                        -> Sort: daily_revenue.order_date  (cost=2.6..2.6 rows=0) (actual time=247..247 rows=611 loops=1)\n                                            -> Table scan on daily_revenue  (cost=2.5..2.5 rows=0) (actual time=247..247 rows=611 loops=1)\n                                                -> Materialize CTE daily_revenue  (cost=0..0 rows=0) (actual time=247..247 rows=611 loops=1)\n                                                    -> Table scan on <temporary>  (actual time=247..247 rows=611 loops=1)\n                                                        -> Aggregate using temporary table  (actual time=247..247 rows=611 loops=1)\n                                                            -> Nested loop inner join  (cost=13189 rows=9614) (actual time=0.0981..210 rows=100756 loops=1)\n                                                                -> Filter: (o.order_status = \'delivered\')  (cost=9845 rows=9532) (actual time=0.093..35.2 rows=96478 loops=1)\n                                                                    -> Table scan on o  (cost=9845 rows=95316) (actual time=0.0915..23.9 rows=99441 loops=1)\n                                                                -> Index lookup on p using PRIMARY (order_id=o.order_id)  (cost=0.25 rows=1.01) (actual time=0.00141..0.0017 rows=1.04 loops=96478)\n'


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

