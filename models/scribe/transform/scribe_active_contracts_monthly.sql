with
	organizations_monthly as (
		select * from {{ ref('scribe_organizations_monthly')}}
	)

select date_month
	, sum(active_contracts) as active_contracts
	, coalesce(
		sum(active_contracts) filter(where is_paid)
		, 0
	) as active_contracts_paid
	, coalesce(sum(active_contracts) filter(where not is_paid)
		, 0
	) as active_contracts_unpaid
from organizations_monthly
group by 1
