    -- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

--

with videos as (
        select * from {{ ref( 'medops_videos_by_org_monthly' ) }}
    )

    , ubi_consults as (
        select * from {{ ref( 'medops_ubisoft_clinic_consultation_count' ) }}
    )

    , paid_employees_monthly as (
        select * from {{ ref( 'users_paid_employees_monthly' ) }}
    )

    select paid_employees_monthly.date_month
        , paid_employees_monthly.organization_name
        , paid_employees_monthly.account_name
        , coalesce(paid_employees_monthly.count_paid_employees, 0) as count_paid_employees
        , coalesce(videos.count_videos, 0) as count_videos
        , coalesce(ubi_consults.nurse_consultations, 0) as nurse_consultations
        , coalesce(ubi_consults.gp_consultations, 0) as gp_consultations
        , coalesce(ubi_consults.mental_health_consultations, 0) as mental_health_consultations
        , coalesce(videos.count_videos, 0) /count_paid_employees::float as video_rate
    from paid_employees_monthly
    left join videos
        on paid_employees_monthly.date_month = videos.date_month
        and paid_employees_monthly.organization_name = videos.organization_name
    left join ubi_consults
        on paid_employees_monthly.date_month = ubi_consults.date_month
        and paid_employees_monthly.organization_name = 'Ubisoft Divertissements Inc. (Bureau de Montreal)'
