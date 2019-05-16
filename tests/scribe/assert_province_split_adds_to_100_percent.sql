{{ config(materialized='ephemeral') }}

{% set provinces = ['Prince Edward Island', 'Alberta', 'Ontario',
	'British Columbia', 'Saskatchewan', 'Manitoba', 'Quebec', 'Nova Scotia',
	'New Brunswick', 'Newfoundland and Labrador', 'Yukon'] %}

with
	province_split as (
		select * from {{ ref('scribe_organization_province_split') }}
	)

select *
	, round(
		{% for province in provinces %}

			{% if not loop.first %} + {% endif %}

			{{ province.lower() | replace(" ","_") }}_perc

		{% endfor %}

		, 4) as total_should_be_one
	,
		{% for province in provinces %}

			{% if not loop.first %}, {% endif %}

			{{ province.lower() | replace(" ","_") }}_perc

		{% endfor %}

from province_split
where round(
		{% for province in provinces %}

			{% if not loop.first %} + {% endif %}

			{{ province.lower() | replace(" ","_") }}_perc

		{% endfor %}

		, 4) <> 1
