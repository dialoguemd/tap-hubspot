with
	opportunities as (
		select *
		from {{ ref('salesforce_opportunities') }}
	),

	accounts as (
		select *
		from {{ ref('salesforce_accounts') }}
	),

	users as (
		select *
		from {{ ref('salesforce_users') }}
	)

select opportunities.*
	, users.name as owner_name
	, users.title as owner_title
	, users.province as owner_province
	, users.started_date as owner_started_date
	, accounts.industry
	, accounts.account_name
	, coalesce(
		accounts.billing_state_code,
		users.state_code
	) as province
	, coalesce(
		accounts.billing_country_code,
		users.country_code
	) as country
from opportunities
inner join accounts
	using (account_id)
inner join users
	on opportunities.owner_id = users.user_id
