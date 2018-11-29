-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with maus as (
		select * from {{ ref('medops_count_maus_by_org') }}
	)

	, daus as (
		select * from {{ ref('medops_count_daus_by_org_monthly') }}
	)

	, paid_employees_monthly as (
		select * from {{ ref('users_paid_employees_monthly') }}
	)

select paid_employees_monthly.date_month
	, paid_employees_monthly.organization_id
	, paid_employees_monthly.organization_name
	, paid_employees_monthly.account_name
	, paid_employees_monthly.count_paid_employees
	, maus.count_mau
	, daus.count_dau
	, case
		when paid_employees_monthly.count_paid_employees <> 0
		then coalesce(maus.count_mau, 0)
			/ paid_employees_monthly.count_paid_employees::float
		else 0
	end as mau_rate
	, case
		when paid_employees_monthly.count_paid_employees <> 0
		then coalesce(daus.count_dau, 0)
			/ paid_employees_monthly.count_paid_employees::float
		else 0
	end as dau_rate
from paid_employees_monthly
left join maus
	using (date_month, organization_id)
left join daus
	using (date_month, organization_id)
