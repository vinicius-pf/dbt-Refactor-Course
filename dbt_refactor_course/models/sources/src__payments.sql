with import as (
    select 
        ORDERID as order_id
        ,max(CREATED) as payment_finalized_date
        ,sum(AMOUNT) / 100.0 as total_amount_paid
    from {{source('main', 'payments')}}
    group by 1

)

select * from import