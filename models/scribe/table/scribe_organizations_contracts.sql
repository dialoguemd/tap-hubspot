with
	contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

	, aggregate as (
		select organization_id
		    , min(during_start) as first_contract_start_date
		    , max(during_end) as last_contract_end_date
		from contracts
		group by 1
	)

select *
	, last_contract_end_date <> '9999-12-31 23:59:59.999999+00' as is_churned
from aggregate
