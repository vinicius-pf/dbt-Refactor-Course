with base_customer as(
    select
        ID as customer_id
        ,FIRST_NAME    as customer_first_name
        ,LAST_NAME as customer_last_name
    from {{source('main', 'customers')}}
)

select * from base_customer