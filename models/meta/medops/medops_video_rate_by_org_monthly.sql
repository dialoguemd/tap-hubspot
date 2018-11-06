-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

--

with monthly_vids_by_org as (
        select * from {{ ref( 'medops_videos_by_org_monthly' ) }}
    )

    , ubi_consults as (
        select * from {{ ref( 'medops_ubisoft_clinic_consultation_count' ) }}
    )

    , paid_employees_monthly as (
        select * from {{ ref( 'users_paid_employees_monthly' ) }}
    )

    select paid_employees_monthly.month
        , paid_employees_monthly.organization_name
        , paid_employees_monthly.account_name
        , coalesce(count_paid_employees, 0) as count_paid_employees
        , coalesce(count_videos, 0) as count_videos
        , coalesce(nurse_consultations, 0) as nurse_consultations
        , coalesce(gp_consultations, 0) as gp_consultations
        , coalesce(mental_health_consultations, 0) as mental_health_consultations
        , coalesce(count_videos, 0) /count_paid_employees::float as video_rate
    from paid_employees_monthly
    left join monthly_vids_by_org as vids
        on paid_employees_monthly.month = vids.month
        and paid_employees_monthly.organization_name = vids.organization_name
    left join ubi_consults
        on paid_employees_monthly.month = ubi_consults.month
        and paid_employees_monthly.organization_name = 'Ubisoft Divertissements Inc. (Bureau de Montreal)'
