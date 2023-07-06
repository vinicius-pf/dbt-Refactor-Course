with base_payments as (
    select 
        orderid as order_id
        ,max(created) as payment_finalized_date
        ,sum(amount) / 100.0 as total_amount_paid
    from {{source('main', 'payments')}}
    where status <> 'fail'
    group by 1

)

select * from base_payments