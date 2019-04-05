with
	usage as (
		select * from {{ ref('client_usage_daily') }}
	)

	, nps as (
		select * from {{ ref('client_nps_daily') }}
	)

	, total_consults as (
		select * from {{ ref('client_total_consults_daily') }}
	)

	, thresholds as (
		select * from {{ ref('client_thresholds') }}
	)

	, joined as (
		select *
		from usage
		left join nps
			using (organization_id, date_day)
		left join total_consults
			using (organization_id, date_day)
	)

select joined.*
	{% for field in
		['invited_employee_count',
		'signed_up_employee_rate',
		'activated_employee_rate',
		'survey_count_cum',
		'survey_avg_cum',
		'total_consults_cum']
	%}
	, case when joined.{{field}} >= thresholds.{{field}}
		then 1 else 0 end as has_sufficient_{{field}}
	{% endfor %}
from joined
left join thresholds
	on joined.days_since_billing_start::integer
		<@ thresholds.days_since_launch_range
