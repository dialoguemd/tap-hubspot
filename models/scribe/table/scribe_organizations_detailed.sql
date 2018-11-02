with
	organization_address as (
		select * from {{ ref('scribe_organization_address') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations') }}
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
	, organizations.is_paid
	, organizations.email_preference
	, organization_address_unique.city
	, organizations.tax_province as province
from organizations
left join organization_address_unique
	using (organization_id)
