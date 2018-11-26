with
	organizations as (
		select * from {{ ref('organizations') }}
	)

select account_id
	, account_name
	, min(billing_start_date) as billing_start_date
from organizations
group by 1,2
