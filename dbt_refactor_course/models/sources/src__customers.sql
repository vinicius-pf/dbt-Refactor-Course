with base_customer as(
    select
        id as customer_id
        ,first_name as customer_first_name
        ,last_name as customer_last_name
    from {{source('main', 'customers')}}
)

select * from base_customer