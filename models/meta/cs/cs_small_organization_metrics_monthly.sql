with
	org_monthly as (
		select * from {{ ref('cs_organization_monthly') }}
	)

	, nps as (
		select * from {{ ref('nps_patient_survey') }}
	)

	, monthly_tmp as (
		select date_month
			, organization_name
			, organization_id
			, account_name
			, billing_start_month
			, billing_start_date
			, months_since_billing_start
			, sum(total_active_on_chat_cum)
				+ sum(total_active_on_video_cum)
				as total_consults
			, sum(employee_invited_count_cum)
				as paid_employees_count
		from org_monthly
		{{ dbt_utils.group_by(n=7) }}
	)

	, monthly as (
		select monthly_tmp.date_month
			, monthly_tmp.organization_name
			, monthly_tmp.organization_id
			, monthly_tmp.account_name
			, monthly_tmp.billing_start_month
			, monthly_tmp.billing_start_date
			, monthly_tmp.months_since_billing_start
			, monthly_tmp.total_consults
			, monthly_tmp.paid_employees_count
			, count(nps.score) as satisfaction_count
			, avg(nps.score)*0.1 as satisfaction_average
		from monthly_tmp
		left join nps
			on monthly_tmp.organization_id = nps.organization_id
			and monthly_tmp.date_month >= date_trunc('month', nps.timestamp)
		{{ dbt_utils.group_by(n=9) }}
	)

select date_month
	, organization_name
	, organization_id
	, account_name
	, billing_start_month
	, billing_start_date
	, months_since_billing_start
	, total_consults
	, coalesce(total_consults = 0, false)
		as has_no_consults
	, paid_employees_count
	, satisfaction_average
	, coalesce(satisfaction_average >= 0.8, false)
		as has_acceptable_satisfaction
	, satisfaction_count
from monthly
where paid_employees_count between 0 and 100
	and months_since_billing_start > 0
