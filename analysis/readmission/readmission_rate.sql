{% set interval = '30 days' %}
with
	episodes as (
		select * from {{ ref('episodes') }}
	)

select count(*) as episode_count
	, 1.0 * count(*) filter(where ep2.episode_id is not null)
		/ count(*) as readmission_rate
from episodes as ep1
left join episodes as ep2
	on ep1.patient_id = ep2.patient_id
	and ep1.issue_type = ep2.issue_type
	and ep1.first_message_patient < ep2.first_message_patient
	and ep1.first_message_patient
		> ep2.first_message_patient - interval '{{interval}}'
	and (
		ep2.first_outcome_category in ('Diagnostic', 'Navigation')
		or ep2.first_outcome = 'referral_without_navigation'
	)
where ep1.first_outcome_category = 'Diagnostic'
	and ep1.first_message_patient < current_date - interval '{{interval}}'
	and ep1.first_message_patient > '2018-10-01'
