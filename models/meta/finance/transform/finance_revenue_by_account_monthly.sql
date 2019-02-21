with
	organizations_monthly as (
		select * from {{ ref('scribe_organizations_monthly') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

	, churn_monthly as (
		select * from {{ ref('finance_churn_monthly') }}
	)

	, accounts_monthly as (
		select organizations.account_id
			, organizations.account_name
			, organizations_monthly.date_month
			, sum(organizations_monthly.price_monthly) as amount
		from organizations_monthly
		inner join organizations
			using (organization_id)
		group by 1,2,3
	)

{% set date_threshold = '2018-01-01' %}
select account_id
	, account_name
	, date_month
	, amount
from accounts_monthly
where date_month < '{{ date_threshold }}'

union all

select account_id
	, account_name
	, date_month
	, amount
from churn_monthly
where date_month >= '{{ date_threshold }}'
