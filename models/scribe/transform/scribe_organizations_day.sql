with
	organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

select generate_series(
	-- Dialogue's launch date
		greatest(date_trunc('month', billing_start_date), '2016-09-01')
		, date_trunc('month', current_date) + interval '1 month' - interval '1 day'
		, '1 day'
	) as date_day
	, organization_id
	, organization_name
	, is_paid
	, billing_start_date
	, charge_strategy
	, charge_price
from organizations
