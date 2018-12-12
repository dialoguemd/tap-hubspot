with user_contract as (
        select * from {{ ref('user_contract') }}
    )
    
    , activated_at as (
        select * from {{ ref('user_activated') }}
    )
    
    , months_tmp as (
        select * from {{ ref('dimension_months') }}
    )
    
    , months as (
        select date_month
        	, tstzrange(date_month, date_month + interval '1 month') as month_range
        from months_tmp
    )
    
    , users as (
        select user_contract.*
            , activated_at.activated_at
            , date_trunc('month', activated_at.activated_at) as activated_month
            , activated_at.activated_at is not null as is_activated
        from user_contract
        left join activated_at
            on user_contract.user_id = activated_at.user_id
            and user_contract.during_end >= activated_at.activated_at
    )

select months.date_month
    , users.organization_name
    , users.organization_id
    , users.account_name
    , users.family_member_type

    , count(users.user_id) 
            as invited_count_cum
    , count(users.user_id) 
        filter (where months.date_month = users.invited_month)
            as invited_count
    , count(users.user_id)
        filter (where users.is_signed_up
            and months.date_month >= users.signed_up_at)
            as signed_up_count_cum
    , count(users.user_id)
        filter (where users.is_signed_up
            and months.date_month = users.signed_up_month)
            as signed_up_count
    , count(users.user_id)
        filter (where users.is_activated
            and months.date_month >= users.activated_at)
            as activated_count_cum
    , count(users.user_id)
        filter (where users.is_activated
            and months.date_month = users.activated_month)
            as activated_count
from months
inner join users
 on months.month_range && users.during
group by 1,2,3,4,5
