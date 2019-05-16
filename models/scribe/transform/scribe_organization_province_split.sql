{% set provinces = ['Prince Edward Island', 'Alberta', 'Ontario',
	'British Columbia', 'Saskatchewan', 'Manitoba', 'Quebec', 'Nova Scotia',
	'New Brunswick', 'Newfoundland and Labrador', 'Yukon'] %}

with
	user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

select organization_id

	{% for province in provinces %}

	, 1.0 * count(*) filter(where residence_province = '{{province}}')
		/ count(*) as {{ province.lower() | replace(" ","_") }}_perc

	{% endfor %}

from user_contract
where is_signed_up
	and is_employee
group by 1
