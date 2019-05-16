{% set provinces = ['Prince Edward Island', 'Alberta', 'Ontario',
	'British Columbia', 'Saskatchewan', 'Manitoba', 'Quebec', 'Nova Scotia',
	'New Brunswick', 'Newfoundland and Labrador', 'Yukon'] %}

with
	organizations_weekly as (
		select * from {{ ref('scribe_organizations_weekly') }}
	)

	, province_split as (
		select * from {{ ref('scribe_organization_province_split') }}
	)

select organizations_weekly.date_week
	, organizations_weekly.organization_id
	, organizations_weekly.organization_name
	, organizations_weekly.price_monthly as mrr

	{% for province in provinces %}

	, organizations_weekly.price_monthly
		* {{province.lower() | replace(" ","_") }}_perc
		as mrr_{{province.lower() | replace(" ","_") }}

	{% endfor %}

from organizations_weekly
inner join province_split
	using (organization_id)
