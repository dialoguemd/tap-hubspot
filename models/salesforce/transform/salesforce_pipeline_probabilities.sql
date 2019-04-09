{% set interval = "interval '90 days'" %}

{% set stages = [
		['meeting', 'Meeting Booked'], ['initiate', 'Initiate'],
		['educate', 'Educate'], ['validate', 'Validate'],
		['justify', 'Justify'], ['decide', 'Decide'],
	]
%}
with
	opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

{% for stage, stage_name in stages %}
	{% if not loop.first %}
		union all
	{% endif %}
select date_trunc('quarter', {{stage}}_date) as date_quarter
	, segment_group
	, case
		when segment_group = '1-99' then 1 / .85
		when segment_group = '100-499' then 1 / .6
		when segment_group = '500+' then 1 / .55
	end as percentile_closed_3_months
	, '{{stage_name}}' as stage_name
	, 1.0
		* count(*) filter(where
			is_won
			and close_date < {{stage}}_date + {{ interval }}
		) / count(*)
	as probability_opp_3_months
	, 1.0
		* sum(amount) filter(where
			is_won
			and close_date < {{stage}}_date + {{ interval }}
		) / nullif(sum(amount), 0)
	as probability_mrr_3_months
	, count(*) as opportunities_count
from opportunities
where {{stage}}_date is not null
	and {{stage}}_date < current_timestamp - {{ interval }}
	and {{stage}}_date >= '2018-01-01'
{{ dbt_utils.group_by(4) }}

{% endfor %}
