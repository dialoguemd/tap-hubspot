with
	organizations_day as (
		select * from {{ ref('scribe_organizations_day') }}
		{% if target.name == 'dev' %}
		  where date_day > current_date - interval '1 months'
		{% endif %}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

select date_trunc('month', organizations_day.date_day) as date_month
	, date_trunc('week', organizations_day.date_day) as date_week
	, organizations_day.date_day
	, organizations_day.organization_id
	, organizations_day.organization_name
	, organizations_day.is_paid
	, organizations_day.billing_start_date
	, organizations_day.charge_strategy
	, organizations_day.charge_price
	, count(distinct contracts.participant_id) as active_contracts
from organizations_day
left join contracts
  on organizations_day.date_day_range && contracts.during
	and organizations_day.organization_id = contracts.organization_id
group by 1,2,3,4,5,6,7,8,9
