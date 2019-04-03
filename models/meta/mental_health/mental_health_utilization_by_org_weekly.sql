with
	user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, costs as (
		select * from {{ ref('costs_by_episode_daily') }}
	)

	, dimension_weeks as (
		select * from {{ ref('dimension_weeks') }}
	)

	, features as (
		select * from {{ ref('scribe_plan_features') }}
	)

	, plans as (
		select * from {{ ref('scribe_plans') }}
	)

	, mh_start as (
		select plans.organization_id
			, features.created_at
		from plans
		inner join features
			using (plan_id)
		where features.feature_id = '4'
	)

	, org_weekly as (
		select dimension_weeks.date_week
			, user_contract.organization_name
			, user_contract.organization_id
			, user_contract.account_id
			, user_contract.account_name
			, count(distinct user_contract.user_id)
				filter(where user_contract.family_member_type = 'Employee'
					and user_contract.charge_price_mental_health > 0)
				as eligible_members_count
			, sum(user_contract.charge_price_mental_health)
				filter(where user_contract.family_member_type = 'Employee')
				/4.33 as est_mh_revenue
		from dimension_weeks
		inner join user_contract
			on dimension_weeks.week_range && user_contract.during
		left join mh_start
			using (organization_id)
		where mh_start.created_at <= upper(dimension_weeks.week_range)
		group by 1,2,3,4,5
	)

	, costs_weekly as (
		select date_trunc('week', costs.date_day) as date_week
			, user_contract.organization_name
			, user_contract.organization_id
			, sum(costs.total_cost) filter (where costs.issue_type = 'psy-pilot') as est_mh_cost
			, count(distinct episode_id) filter (where costs.issue_type = 'psy-pilot') as active_episodes
		from costs
		left join user_contract
			on costs.user_id = user_contract.user_id
			and costs.date_day <@ user_contract.during_est
		group by 1,2,3
	)

select org_weekly.date_week
	, org_weekly.organization_name
	, org_weekly.organization_id
	, org_weekly.account_name
	, org_weekly.account_id
	, org_weekly.est_mh_revenue
	, costs_weekly.est_mh_cost
	, (org_weekly.est_mh_revenue - costs_weekly.est_mh_cost) *1.0
		/ org_weekly.est_mh_revenue as est_mh_gm
	, org_weekly.eligible_members_count
	, costs_weekly.active_episodes
from org_weekly
left join costs_weekly
	using (organization_id, date_week)
where org_weekly.date_week > '2018-10-01'
