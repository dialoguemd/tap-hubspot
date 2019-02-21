with
	post_submitted as (
		select * from {{ ref('post_submitted') }}
	)

select date_trunc('month', timestamp) as date_month
	, patient_id
	, account_id
	, account_name
	, organization_id
	, organization_name
	, gender
	, language
	, family_member_type
	, residence_province
	, age
	, platform_name
from post_submitted
{{ dbt_utils.group_by(12) }}
