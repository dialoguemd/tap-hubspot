-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with maus as (
        select * from {{ ref( 'medops_count_maus_by_org' ) }}
    )

    , daus as (
        select * from {{ ref( 'medops_count_daus_by_org_monthly' ) }}
    )

    , paid_employees_monthly as (
        select * from {{ ref( 'users_paid_employees_monthly' ) }}
    )

    select paid_employees_monthly.date_month
        , paid_employees_monthly.organization_name
        , paid_employees_monthly.account_name
        , paid_employees_monthly.count_paid_employees
        , maus.count_mau
        , daus.count_dau
        , coalesce(maus.count_mau, 0)/paid_employees_monthly.count_paid_employees::float as mau_rate
        , coalesce(daus.count_dau, 0)/paid_employees_monthly.count_paid_employees::float as dau_rate
    from paid_employees_monthly
    left join maus
        on paid_employees_monthly.date_month = maus.date_month
        and paid_employees_monthly.organization_name = maus.organization_name
    left join daus
        on paid_employees_monthly.date_month = daus.date_month
        and paid_employees_monthly.organization_name = daus.organization_name
