with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, organization_weekly as (
		select * from {{ ref('scribe_organization_mrr_by_province_weekly') }}
	)

select dates.date_week
    , sum(organization_weekly.mrr) as mrr
    , sum(organization_weekly.mrr_qc) as mrr_qc
    , sum(organization_weekly.mrr_on) as mrr_on
    , sum(organization_weekly.mrr_roc) as mrr_roc
    , sum(organization_weekly.mrr_qc) / sum(organization_weekly.mrr) as mrr_qc_perc
    , sum(organization_weekly.mrr_on) / sum(organization_weekly.mrr) as mrr_on_perc
    , sum(organization_weekly.mrr_roc) / sum(organization_weekly.mrr) as mrr_roc_perc
from dates
left join organization_weekly
	on dates.date_week = organization_weekly.date_week
		and organization_weekly.date_week < date_trunc('week', current_date)
group by 1
