with
    organizations_weekly as (
        select * from {{ ref('scribe_organizations_weekly') }}
    )

    , province_split as (
        select * from {{ ref('scribe_organization_province_split') }}
    )

select organizations_weekly.date_week
    , organizations_weekly.organization_id
    , organizations_weekly.organization_name
    , organizations_weekly.price_monthly as mrr
    , organizations_weekly.price_monthly * qc_perc as mrr_qc
    , organizations_weekly.price_monthly * on_perc as mrr_on
    , organizations_weekly.price_monthly * roc_perc as mrr_roc
    , province_split.qc_perc
    , province_split.on_perc
    , province_split.roc_perc
from organizations_weekly
inner join province_split
    using (organization_id)
