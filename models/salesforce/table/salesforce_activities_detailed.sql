with
    activities as (
        select *
        from {{ ref('salesforce_activities') }}
    ),

    users as (
        select *
        from {{ ref('salesforce_users') }}
    ),

    opportunities_direct as (
        select *
        from {{ ref('salesforce_opportunities_detailed_direct') }}
    ),

    sf_accounts as (
        select *
        from {{ ref('salesforce_accounts') }}
    ),

    accounts as (
        select sf_accounts.account_id
            , sf_accounts.account_name
            , sf_accounts.billing_state_code as province
            , sf_accounts.billing_country_code as country
            , sf_accounts.industry
            , sf_accounts.number_of_employees
            , sf_accounts.mrr
            , max(opportunities_direct.number_of_employees) as opps_number_of_employees
            , max(opportunities_direct.amount) as opps_amount
            , min(opportunities_direct.meeting_date) as meeting_date
            , bool_or(opportunities_direct.is_won) as is_won
        from sf_accounts
        left join opportunities_direct
            using (account_id)
        group by 1,2,3,4,5,6,7
    )

select activities.activity_id
    , accounts.account_id
    , accounts.account_name
    , accounts.meeting_date
    , accounts.province
    , accounts.country
    , accounts.industry
    , users.user_id as owner_id
    , users.user_name as owner_name
    , activities.activity_date
    , activities.type
    , activities.task_subtype
    , accounts.is_won as account_is_won
    , coalesce(
        accounts.number_of_employees
        , accounts.opps_number_of_employees
    ) as number_of_employees
    , coalesce(accounts.mrr, accounts.opps_amount)
        as amount
from activities
inner join users
    on activities.owner_id = users.user_id
left join accounts
    using (account_id)
where activities.status = 'Completed'
    and users.title in ('Account Executive', 'Enterprise Account Executive', 'SDR')
    and (accounts.meeting_date is null
        or accounts.meeting_date >= activities.activity_date)
