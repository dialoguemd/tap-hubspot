select *
from {{ ref('salesforce_opportunities_detailed') }}
where (is_won or stage_name = 'Decide')
	and launch_date > date_trunc('week', current_date) - interval '2 weeks'
