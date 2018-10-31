with
    activities as (
        select * from {{ ref('salesforce_activities') }}
    )

    , users as (
        select * from {{ ref('salesforce_users') }}
    )

    , accounts_detailed as (
        select * from {{ ref('salesforce_accounts_detailed') }}
    )

select activities.activity_id
    , accounts_detailed.account_id
    , accounts_detailed.account_name
    , accounts_detailed.meeting_date
    , accounts_detailed.province
    , accounts_detailed.country
    , accounts_detailed.industry
    , users.user_id as owner_id
    , users.user_name as owner_name
    , activities.activity_date
    , activities.type
    , activities.task_subtype
    , accounts_detailed.is_won as account_is_won
    , coalesce(
        accounts_detailed.number_of_employees
        , accounts_detailed.opps_number_of_employees
    ) as number_of_employees
    , coalesce(accounts_detailed.mrr, accounts_detailed.opps_amount)
        as amount
from activities
inner join users
    on activities.owner_id = users.user_id
left join accounts_detailed
    using (account_id)
where activities.status = 'Completed'
    and users.title in ('Account Executive', 'Enterprise Account Executive', 'SDR')
    and (accounts_detailed.meeting_date is null
        or accounts_detailed.meeting_date >= activities.activity_date)
