with
	organizations as (
		select * from {{ ref('organizations') }}
	)

	, aggregate as (
		select account_id
			, account_name
			, min(billing_start_date) as billing_start_date
			, min(first_contract_start_date) as first_contract_start_date
			, max(last_contract_end_date) as last_contract_end_date
			, bool_and(is_churned) as is_churned
		from organizations
		group by 1,2
	)

select *
	, date_trunc('month', billing_start_date)
		as billing_start_month
	, date_trunc('month', first_contract_start_date)
		as first_contract_start_month
	, date_trunc('month', last_contract_end_date)
		as last_contract_end_month
	, case
		when is_churned
		then last_contract_end_date
		else null
	end as churn_date
	, case
		when is_churned
		then date_trunc('month', last_contract_end_date)
		else null
	end as churn_month
from aggregate
