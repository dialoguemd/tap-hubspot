
{{
  config(
    materialized='incremental',
    unique_key='usage_id',
    post_hook=[
       "{{ postgres.index(this, 'usage_id')}}",
    ]
  )
}}

with
	users as (
		select * from {{ ref('user_contract')}}
	)

	, organization_days as (
		select * from {{ ref('client_organization_days')}}
		{% if is_incremental() %}
		where date_day > (select max(date_day) from {{ this }})
		{% endif %}
	)

	, usage_daily as (
		select organization_days.date_day
			, organization_days.organization_id
			, organization_days.organization_name_id
			, organization_days.account_manager_email
			, organization_days.account_name
			, organization_days.billing_start_date

			{% for gender in ['m', 'f'] %}

			, count(distinct users.user_id)
				filter (where date_trunc('day', users.activated_at) <= organization_days.date_day
					and lower(users.gender) = '{{gender}}')
					as {{gender}}_count

			{% endfor %}

			, percentile_cont(0.5) within group
				(order by extract(year from age(organization_days.date_day, users.birthday)) asc)
				as median_age

			{% for n in [5,10,15,20,25,30,35,40,45,50,55,60,65] %}

			, case when
				count(distinct users.user_id)
					filter (where date_trunc('day', users.activated_at) <= organization_days.date_day) > 0
				then
					count(distinct users.user_id)
						filter (where date_trunc('day', users.activated_at) <= organization_days.date_day
							and extract(year from age(organization_days.date_day, users.birthday))::integer
								<@ int4range({{n}} - 5, {{n}})) * 1.0 /
						count(distinct users.user_id)
							filter (where date_trunc('day', users.activated_at) <= organization_days.date_day)
				else 0
				end as age_bin_{{n}}_count

			{% endfor %}

			, case when
				count(distinct users.user_id)
					filter (where date_trunc('day', users.activated_at) <= organization_days.date_day) > 0
				then
					count(distinct users.user_id)
						filter (where date_trunc('day', users.activated_at) <= organization_days.date_day
							and extract(year from (organization_days.date_day - users.birthday)) > 65)
				else 0
				end as age_over_65_count

			, count(distinct users.user_id)
				filter (where users.family_member_type = 'Employee')
				as invited_employee_count

			{% for fmt in ['employee', 'dependent', 'child'] %}
			, count(distinct users.user_id)
				filter (where lower(users.family_member_type) = '{{fmt}}'
					and date_trunc('day', users.signed_up_at) <= organization_days.date_day)
				as signed_up_{{fmt}}_count
			{% endfor %}
			, count(distinct users.user_id)
				filter (where users.family_member_type <> 'Employee'
					and date_trunc('day', users.signed_up_at) <= organization_days.date_day)
				as signed_up_family_member_count

			{% for fmt in ['employee', 'dependent', 'child'] %}
			, count(distinct users.user_id)
				filter (where lower(users.family_member_type) = '{{fmt}}'
					and date_trunc('day', users.activated_at) <= organization_days.date_day)
				as activated_{{fmt}}_count
			{% endfor %}
			, count(distinct users.user_id)
				filter (where users.family_member_type <> 'Employee'
					and date_trunc('day', users.activated_at) <= organization_days.date_day)
				as activated_family_member_count

		from organization_days
		left join users
			on organization_days.organization_id = users.organization_id
			and organization_days.date_day <@ users.during
		{{ dbt_utils.group_by(6) }}
	)

select md5(organization_id::text || date_day) as usage_id
	, date_day
	, extract('days' from (date_day - billing_start_date)) as days_since_billing_start
	, organization_id
	, organization_name_id
	, account_manager_email
	, account_name
	, m_count
	, f_count
	, median_age
	{% for n in [5,10,15,20,25,30,35,40,45,50,55,60,65] %}
	, age_bin_{{n}}_count
	{% endfor %}
	, age_over_65_count
	, invited_employee_count
	, signed_up_employee_count
	, signed_up_family_member_count
	, case
		when invited_employee_count > 0
			then (signed_up_employee_count * 1.0 / invited_employee_count)
		else 0
		end as signed_up_employee_rate
	, activated_employee_count
	, activated_family_member_count
	, case
		when signed_up_employee_count > 0
			then (activated_employee_count * 1.0 / signed_up_employee_count)
		else 0
		end as activated_employee_rate
from usage_daily
