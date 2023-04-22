with first_payments as (
select user_id
    ,min(date_trunc('day', transaction_datetime)) as first_payment_date 
from skyeng_db.payments
where status_name = 'success'
group by user_id)

,all_dates as (
select distinct date_trunc('day', class_start_datetime) as dt
from skyeng_db.classes
where class_start_datetime >= '2016-01-01' and class_start_datetime < '2017-01-01')

,payments_by_dates as (
select user_id  
    ,date_trunc('day', transaction_datetime) as payment_date
    ,sum(classes) as transaction_balance_change
from skyeng_db.payments
where status_name = 'success'
group by user_id, transaction_datetime)

,all_dates_by_user as (
select user_id
    ,dt
from all_dates a 
    left join first_payments b 
    on a.dt >= b.first_payment_date
    where user_id is not null)
    
,payments_by_dates_cumsum as
(select
        ad.user_id,
        ad.dt,
        pd.transaction_balance_change,
        sum(coalesce(pd.transaction_balance_change, 0))
            over (partition by ad.user_id order by ad.dt rows between unbounded preceding and current row) as transaction_balance_change_cs
    from all_dates_by_user ad
        left join payments_by_dates pd
            on pd.payment_date = ad.dt
            and pd.user_id = ad.user_id
    order by ad.user_id, ad.dt
)

,classes_by_dates as (select user_id
    ,date_trunc('day', class_start_datetime) as class_date
    ,count(class_start_datetime) * -1 as classes
from skyeng_db.classes
where (class_status = 'failed_by_student' or class_status = 'success') and class_type != 'trial'
group by user_id, class_date)


,classes_by_dates_dates_cumsum as (
select ad.user_id
    ,ad.dt
    ,pd.classes
    ,sum(coalesce(pd.classes, 0))
            over (partition by ad.user_id order by ad.dt rows between unbounded preceding and current row) as classes_cs
from all_dates_by_user ad
    left join classes_by_dates pd
    on ad.user_id = pd.user_id
    and ad.dt = pd.class_date
order by ad.user_id, ad.dt
    )

,balances as (
select a.user_id
    , a.dt
    , transaction_balance_change
    , transaction_balance_change_cs
    , classes
    , classes_cs
    , classes_cs + transaction_balance_change_cs as balance
from payments_by_dates_cumsum a
    join classes_by_dates_dates_cumsum b
    on (a.user_id = b.user_id)
    and (a.dt = b.dt))
    
select dt
    , sum(transaction_balance_change) as sum_by_date_transaction_balance_change
    , sum(transaction_balance_change_cs) as sum_by_date_transaction_balance_change_cs
    , sum(classes) as sum_by_date_classes
    , sum(classes_cs) as sum_by_date_classes_cs
    , sum(balance) as sum_by_date_balance
from balances
group by dt
order by dt
