with
	organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

select organizations.organization_id
	, organizations.organization_name
	, organizations.email_preference
	, organizations.billing_start_date
	, organizations.is_paid
from organizations
inner join contracts
	using (organization_id)
where current_timestamp <@ contracts.during
group by 1,2,3,4,5
