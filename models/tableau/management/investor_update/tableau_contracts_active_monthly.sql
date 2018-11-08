with
    active_users as (
        select * from {{ ref('active_users')}}
    )

    , organizations_monthly as (
        select * from {{ ref('scribe_organizations_monthly')}}
    )

    , active_monthly as (
        select date_month
            , count(distinct patient_id)
                filter(where contract_status = 'paid') as active_users_paid_monthly
        from active_users
        group by 1
    )
    
    , contracts_monthly as (
        select date_month, sum(active_contracts) as active_contracts_paid
        from organizations_monthly
        where is_paid
        group by 1
    )
    
select *
from contracts_monthly
inner join active_monthly
    using (date_month)
