with user_contract as (
        select * from {{ ref('user_contract') }}
    )
    
    , activated_at as (
        select * from {{ ref('user_activated') }}
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

    , org_months_tmp as (
        select organization_id
            , organization_name
            , account_name
            , billing_start_date
            , residence_province
            , generate_series(
                date_trunc('month', billing_start_date),
                date_trunc('month', current_date),
                interval '1 month') as date_month
        from user_contract
        group by 1,2,3,4,5,6
    )

    , org_months as (
        select date_month
            , organization_id
            , residence_province
            , organization_name
            , account_name
            , billing_start_date
            , tstzrange(date_month, date_month + interval '1 month') as month_range
        from org_months_tmp
    )

select org_months.date_month
    , org_months.organization_name
    , org_months.organization_id
    , org_months.account_name
    , org_months.residence_province
    , date_trunc('month', org_months.billing_start_date)
        as billing_start_month
    , users.family_member_type

    -- Count distinct to not double count users with multiple contracts
    -- in the same month
    , count(distinct users.user_id)
            as invited_count_cum
    , count(distinct users.user_id)
        filter (where org_months.date_month = users.invited_month)
            as invited_count
    , count(distinct users.user_id)
        filter (where users.is_signed_up
            and org_months.date_month >= users.signed_up_month)
            as signed_up_count_cum
    , count(distinct users.user_id)
        filter (where users.is_signed_up
            and org_months.date_month = users.signed_up_month)
            as signed_up_count
    , count(distinct users.user_id)
        filter (where users.is_activated
            and org_months.date_month >= users.activated_month)
            as activated_count_cum
    , count(distinct users.user_id)
        filter (where users.is_activated
            and org_months.date_month = users.activated_month)
            as activated_count
from org_months
left join users
    on users.organization_id = org_months.organization_id
    and users.residence_province = org_months.residence_province
    and org_months.month_range && users.during
group by 1,2,3,4,5,6,7
