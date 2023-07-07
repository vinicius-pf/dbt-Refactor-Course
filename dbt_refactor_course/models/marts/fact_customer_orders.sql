WITH 

/*Cosmetic CleanUps and CTE grouping*/

 base_paid_orders as (
    select *
    from {{ref('paid_orders')}}
)


, base_customer_orders  as (
    select *
    from {{ref('customer_orders')}} 
)

, total_amount_per_order as (
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
total_amount_per_order.clv_bad as customer_lifetime_value,
c.first_order_date as fdos
FROM base_paid_orders p
left join base_customer_orders as c USING (customer_id)
LEFT OUTER JOIN total_amount_per_order on total_amount_per_order.order_id = p.order_id
ORDER BY total_amount_per_order.order_id