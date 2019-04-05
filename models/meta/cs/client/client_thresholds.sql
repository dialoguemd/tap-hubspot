select days_since_launch
	, int4range(days_since_launch,
		coalesce(lead(days_since_launch) over (order by days_since_launch)
			,99999)) as days_since_launch_range
	, invited_employee_count
	, signed_up_employee_rate
	, activated_employee_rate
	, survey_count_cum
	, average_score as survey_avg_cum
	, total_consults as total_consults_cum
from {{ ref('data_cs_client_thresholds') }}
