with
	nps_survey as (
		select * from {{ ref('delighted_nps_patient_survey') }}
	)

select date_week
	, round(100.0 * (count(*) filter(where category = 'promoter')
			- count(*) filter (where category = 'detractor')
		) / count(*)) as nps
	, count(*) filter(where category = 'promoter') as promoter_count
	, count(*) filter(where category = 'passive') as passive_count
	, count(*) filter(where category = 'detractor') as detractor_count
	, count(*) as respondent_count
from nps_survey
group by 1
