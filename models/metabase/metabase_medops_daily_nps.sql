with
	nps_patient_survey as (
		select * from {{ ref('nps_patient_survey') }}
	)

select
	case
		when residence_province in ('Quebec', 'Ontario')
		then residence_province
		when residence_province in (
			'Alberta',
			'British Columbia',
			'Manitoba',
			'Saskatchewan'
		) then 'Western Canada'
		when residence_province in (
			'Prince Edward Island',
			'New Brunswick',
			'Newfoundland and Labrador',
			'Nova Scotia'
		) then 'Eastern Canada'
		else 'North Territories'
	end as province
	, (
		100.0 * (
			count(*) filter(where category = 'promoter')
			- count(*) filter(where category = 'detractor')
		)
		/ count(*)
	)::integer as nps
	, count(*) as responses
from nps_patient_survey
where date_trunc('day', timestamp) = current_date - interval '1 day'
group by 1
