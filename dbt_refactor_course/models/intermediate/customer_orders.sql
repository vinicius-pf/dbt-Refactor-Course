with 

customers as (
    select *
    from {{ref('src__customers')}}

)

, orders as (
    select *
    from {{ref('src__orders')}}

)

, joining_tables as (
    select 
        customers.customer_id
        , min(orders.order_placed_at) as first_order_date
        , max(orders.order_placed_at) as most_recent_order_date
        , count(orders.order_id) AS number_of_orders
    from customers  
    left join orders on orders.customer_id = customers.customer_id
    group by 1 
)


select * from joining_tables