with
	plans as (
		select * from {{ ref('scribe_plans') }}
	)

	, plan_feature_junction as (
		select * from {{ ref('scribe_plan_features') }}
	)

	, features as (
		select * from {{ ref('scribe_features') }}
	)

	, plan_features as (
		select plan_feature_junction.plan_id
			, string_agg(features.feature_name, ', ') as features
			, bool_or(features.feature_name = '24_7') as has_24_7
			, bool_or(features.feature_name = 'mental_health') as has_mental_health
		from plan_feature_junction
		left join features
			using (feature_id)
		group by 1
	)

select plans.plan_id
	, plans.organization_id
	, plans.created_at
	, plans.during
	, plans.description_en
	, plans.description_fr
	, plans.plan_name
	, plans.plan_name_fr
	, plans.stripe_plan_id
	, plans.charge_strategy
	, plans.charge_price as charge_price
	, case when plan_features.has_mental_health
		then 5
		else 0
		end as charge_price_mental_health
	, case when plan_features.has_24_7
		-- The current price is the old price plus 17%
		then plans.charge_price * 0.17 / 1.17 :: float
		else 0
		end as charge_price_24_7
	, plan_features.features
	, plan_features.has_mental_health
	, plan_features.has_24_7
from plans
left join plan_features
	using (plan_id)
