with user_contract as (
        select * from {{ ref('user_contract') }}
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
        {{ dbt_utils.group_by(n=6) }}
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
    , org_months.billing_start_date
    , user_contract.family_member_type

    -- Count distinct to not double count user_contract with multiple contracts
    -- in the same month
    , count(distinct user_contract.user_id)
            as invited_count_cum
    , count(distinct user_contract.user_id)
        filter (where org_months.date_month = user_contract.invited_month)
            as invited_count
    , count(distinct user_contract.user_id)
        filter (where user_contract.is_signed_up
            and org_months.date_month >= user_contract.signed_up_month)
            as signed_up_count_cum
    , count(distinct user_contract.user_id)
        filter (where user_contract.is_signed_up
            and org_months.date_month = user_contract.signed_up_month)
            as signed_up_count
    , count(distinct user_contract.user_id)
        filter (where user_contract.has_first_message
            and org_months.date_month >= user_contract.first_message_month)
            as activated_count_cum
    , count(distinct user_contract.user_id)
        filter (where user_contract.has_first_message
            and org_months.date_month = user_contract.first_message_month)
            as activated_count
from org_months
left join user_contract
    on user_contract.organization_id = org_months.organization_id
    and user_contract.residence_province = org_months.residence_province
    and org_months.month_range && user_contract.during
{{ dbt_utils.group_by(n=8) }}
