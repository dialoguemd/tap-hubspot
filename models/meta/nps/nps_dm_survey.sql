with
	typeform_nps as (
		select * from {{ ref('typeform_nps_dm_survey') }}
	)

	, delighted_nps as (
		select * from {{ ref('delighted_survey_decision_maker') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

	, nps_all as (
		select email
			, organization_id
			, score
			, category
			, comment
			, timestamp
			, updated_at
			, contact_type
			, month_since_billing_start_date
		from typeform_nps
		union all
		select email
			, organization_id
			, score
			, category
			, comment
			, timestamp
			, updated_at
			, contact_type
			, month_since_billing_start_date
		from delighted_nps
	)

select nps_all.*
	, date_trunc('month', timestamp) as date_month
	, coalesce(organizations.organization_name, 'N/A') as organization_name
	, coalesce(organizations.account_name, 'N/A') as account_name
from nps_all
left join organizations
	using (organization_id)
