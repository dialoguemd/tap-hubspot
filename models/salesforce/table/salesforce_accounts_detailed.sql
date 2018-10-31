with
    accounts as (
        select * from {{ ref('salesforce_accounts') }}
    )

    , opportunities_direct as (
        select * from {{ ref('salesforce_opportunities_detailed_direct') }}
    )

select accounts.account_id
    , accounts.account_name
    , accounts.billing_state_code as province
    , accounts.billing_country_code as country
    , accounts.industry
    , accounts.number_of_employees
    , accounts.mrr
    , max(opportunities_direct.number_of_employees) as opps_number_of_employees
    , max(opportunities_direct.amount) as opps_amount
    , min(opportunities_direct.meeting_date) as meeting_date
    , bool_or(opportunities_direct.is_won) as is_won
from accounts
left join opportunities_direct
    using (account_id)
group by 1,2,3,4,5,6,7
