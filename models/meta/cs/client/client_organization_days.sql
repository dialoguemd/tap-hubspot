
{{
  config(
    materialized='table'
  )
}}

with
	organizations as (
		select * from {{ ref('organizations')}}
	)

select organization_id
	, organization_id::text || ' - ' 	|| organization_name as organization_name_id
	, account_manager_email
	, account_name
	, billing_start_date
	, generate_series(billing_start_date
		, current_date - interval '1 day'
		, '1 day') as date_day
from organizations
