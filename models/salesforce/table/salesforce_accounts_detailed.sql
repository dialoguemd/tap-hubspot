with
    accounts as (
        select * from {{ ref('salesforce_accounts') }}
    )

    , opportunities_direct as (
        select * from {{ ref('salesforce_opportunities_detailed_direct') }}
    )

    , users as (
        select * from {{ ref('salesforce_users') }}
    )

select accounts.account_id
    , accounts.account_name
    , accounts.billing_state_code as province
    , accounts.billing_country_code as country
    , accounts.industry
    , accounts.number_of_employees
    , accounts.mrr
    , accounts.sdr_id
    , sdr.name as sdr_name
    , accounts.am_id
    , am.name as am_name
    , am.email as am_email
    , max(opportunities_direct.number_of_employees) as opps_number_of_employees
    , max(opportunities_direct.amount) as opps_amount
    , min(opportunities_direct.meeting_date) as meeting_date
    , bool_or(opportunities_direct.is_won) as is_won
from accounts
left join opportunities_direct
    using (account_id)
left join users as sdr
    on accounts.sdr_id = sdr.user_id
left join users as am
    on accounts.am_id = am.user_id
{{ dbt_utils.group_by(n=12) }}
