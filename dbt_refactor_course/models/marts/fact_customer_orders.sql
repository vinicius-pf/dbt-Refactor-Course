WITH 
/*Import CTEs*/

base_customers as (
    select *
    from {{ref('src__customers')}}
)

, base_payments as (
    select *
    from {{ref('src__payments')}}
)

, base_orders as (
    select *
    from {{ref('src__orders')}}
)

---------------------------




------------------------
, paid_orders as (select base_orders.order_id,
    base_orders.customer_id,
    base_orders.order_placed_at,
    base_orders.order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.customer_first_name,
    C.customer_last_name
FROM base_orders
left join base_payments p ON base_orders.order_id = p.order_id
left join base_customers C on base_orders.customer_id = C.customer_id )

, customer_orders  as (select C.customer_id
    , min(base_orders.order_placed_at) as first_order_date
    , max(base_orders.order_placed_at) as most_recent_order_date
    , count(base_orders.order_id) AS number_of_orders
from base_customers C 
left join base_orders 
on base_orders.customer_id = C.customer_id  
group by 1)

select
p.*,
ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
CASE WHEN c.first_order_date = p.order_placed_at
THEN 'new'
ELSE 'return' END as nvsr,
x.clv_bad as customer_lifetime_value,
c.first_order_date as fdos
FROM paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN 
(
        select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
ORDER BY x.order_id