WITH 
/*Import CTEs*/

customers as (
    select *
    from {{ref('src__customers')}}
)

, payments as (
    select *
    from {{ref('src__payments')}}
)

, orders as (
    select *
    from {{ref('src__orders')}}
)

/*Cosmetic CleanUps and CTE grouping*/



, base_paid_orders as (
    select *
    from {{ref('paid_orders')}}
)


, customer_orders  as (
    select 
        customers.customer_id
        , min(orders.order_placed_at) as first_order_date
        , max(orders.order_placed_at) as most_recent_order_date
        , count(orders.order_id) AS number_of_orders
    from customers  
    left join orders 
    on orders.customer_id = customers.customer_id  
    group by 1
)

, x as (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from base_paid_orders p
    left join base_paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
)

---------------------------




------------------------


select
p.*,
ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
CASE WHEN c.first_order_date = p.order_placed_at
THEN 'new'
ELSE 'return' END as nvsr,
x.clv_bad as customer_lifetime_value,
c.first_order_date as fdos
FROM base_paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN x on x.order_id = p.order_id
ORDER BY x.order_id