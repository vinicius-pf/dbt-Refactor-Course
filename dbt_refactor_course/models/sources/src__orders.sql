with base_orders as (
    select 
        id as order_id
        ,user_id	as customer_id
        ,order_date AS order_placed_at
        ,status AS order_status
    from {{source('main', 'orders')}}

)

select * from base_orders