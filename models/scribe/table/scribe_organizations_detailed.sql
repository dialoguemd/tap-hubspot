with
	organization_address as (
		select * from {{ ref('scribe_organization_address') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations') }}
	)

	, plans as (
		select * from {{ ref('scribe_plans') }}
	)

	, organization_address_rank as (
		select organization_id
			, organization_address_id
			, city
			, row_number() over (
				partition by organization_id
				order by organization_address_id desc
			) as rank
		from organization_address
	)

	, organization_address_unique as (
		select organization_id
			, city
		from organization_address_rank
		where rank = 1
	)

select organizations.organization_id
	, organizations.organization_name
	, organizations.billing_start_date
	, coalesce(
		organizations.billing_start_date <> '1970-01-01'
		and plans.charge_price <> 0, false) as is_paid
	, organizations.email_preference
	, organization_address_unique.city
	, organizations.tax_province as province
	, case
		when organizations.billing_start_date = '1970-01-01'
		then 'free'
		else plans.charge_strategy
	end as charge_strategy
	, case
		when organizations.billing_start_date = '1970-01-01'
		then 0
		else plans.charge_price
	end as charge_price
from organizations
left join organization_address_unique
	using (organization_id)
left join plans
	on organizations.organization_id = plans.organization_id
