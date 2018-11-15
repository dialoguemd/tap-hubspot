with
	organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts_detailed') }}
	)

	, active_users as (
		select * from {{ ref('active_users') }}
	)

	, org_start_month as (
		select organization_id
			, organization_name
			, billing_start_date
			, generate_series(
				billing_start_date
				, current_date
				, interval '1 month'
			) as month_start
		from organizations
		where is_paid
	)

	, org_dates as (
		select organization_id
			, organization_name
			, billing_start_date
			, month_start
			, month_start + interval '1 month' as month_end
			, row_number() over(
				partition by organization_id
				order by month_start
			) as month_since_start
		from org_start_month
	)

	, org_contracts as (
		select org_dates.organization_id
			, org_dates.organization_name
			, org_dates.billing_start_date
			, org_dates.month_start
			, org_dates.month_end
			, org_dates.month_since_start
			, count(distinct contracts.participant_id) as contracts
		from org_dates
		left join contracts
			on org_dates.month_end <@ contracts.during
				and org_dates.organization_id = contracts.organization_id
		where (date_trunc('month', org_dates.billing_start_date)
				+ interval '1 month' * org_dates.month_since_start)
			< date_trunc('month', current_date)
		group by 1,2,3,4,5,6
	)

	, active_users_agg as (
		select org_dates.organization_id
			, org_dates.month_since_start
			, count(distinct active_users.user_id) as active_users
			, count(distinct active_users.dau_id) as daus
		from org_dates
		left join active_users
			on org_dates.month_start <= active_users.date_day
				and org_dates.month_end > active_users.date_day
				and org_dates.organization_id = active_users.organization_id
		group by 1,2
	)

select org_contracts.organization_id
	, org_contracts.organization_name
	, org_contracts.billing_start_date
	, org_contracts.month_start
	, org_contracts.month_end
	, org_contracts.month_since_start
	, org_contracts.contracts
	, active_users_agg.active_users
	, active_users_agg.daus
from org_contracts
inner join active_users_agg
	using (organization_id, month_since_start)
