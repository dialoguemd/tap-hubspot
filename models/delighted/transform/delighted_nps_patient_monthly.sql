with
	nps_survey as (
		select * from {{ ref('delighted_survey_patient') }}
	)

select date_month
	, round(100.0 * (count(*) filter(where category = 'promoter')
			- count(*) filter (where category = 'detractor')
		) / count(*)) as nps
	, count(*) filter(where category = 'promoter') as promoter_count
	, count(*) filter(where category = 'passive') as passive_count
	, count(*) filter(where category = 'detractor') as detractor_count
	, count(*) as respondent_count
from nps_survey
where date_month < date_trunc('month', current_date)
group by 1
