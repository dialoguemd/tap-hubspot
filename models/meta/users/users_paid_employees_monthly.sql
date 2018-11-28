with
    organizations_monthly as (
        select * from {{ ref('organizations_monthly') }}
    )

select date_month
    , organization_id
    , organization_name
    , account_id
    , account_name
    , active_contracts as count_paid_employees
from organizations_monthly
