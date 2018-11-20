with
	contracts_daily as (
		select * from {{ ref('scribe_contracts_daily') }}
	)

select date_trunc('week', date_day) as date_week
	, avg(contract_count) as contract_count
	, avg(contract_paid_count) as contract_paid_count
from contracts_daily
group by 1
