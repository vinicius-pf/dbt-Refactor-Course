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


, joining_tables as (
    
    select 
        orders.order_id
        ,orders.customer_id
        ,orders.order_placed_at
        ,orders.order_status
        ,payments.total_amount_paid
        ,payments.payment_finalized_date
        ,customers.customer_first_name
        ,customers.customer_last_name
    FROM orders
    left join payments ON orders.order_id = payments.order_id
    left join customers on orders.customer_id = customers.customer_id 

)

select * from joining_tables