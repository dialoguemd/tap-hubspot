-- Set partition definition to use later
{%
	set partition =
		'(partition by organization_days.organization_name_id
		order by organization_days.date_day)'
%}

with
	nps_survey as (
		select * from {{ ref('nps_patient_survey') }}
	)

	, organization_days as (
		select * from {{ ref('client_organization_days')}}
	)

	, nps_daily as (
		select date_trunc('day', timestamp) as date_day
			, organization_id
			, count(*) as survey_count
			, sum(score) as score_sum
			, count(*) filter (where category = 'promoter') as survey_promoter_count
			, count(*) filter (where category = 'passive') as survey_passive_count
			, count(*) filter (where category = 'detractor') as survey_detractor_count
		from nps_survey
		group by 1,2
	)

select organization_days.date_day
	, organization_days.organization_id
	, sum(nps_daily.survey_count)
		over {{partition}}
		as survey_count_cum
	, sum(nps_daily.score_sum)
		over {{partition}}
		as survey_sum_cum
	, sum(nps_daily.score_sum)
		over {{partition}}
		/ sum(nps_daily.survey_count)
		over {{partition}}
		as survey_avg_cum

	{% for type in var("nps_category") %}
	, sum(nps_daily.survey_{{type}}_count)
		over {{partition}}
		as survey_{{type}}_count_cum
	{% endfor %}

from organization_days
left join nps_daily
	using (organization_id, date_day)
