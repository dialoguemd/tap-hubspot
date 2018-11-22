with
	post_submitted as (
		select * from {{ ref('patientapp_submit_post_success') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select post_submitted.*
	, episodes.patient_id
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.gender
	, user_contract.language
	, user_contract.family_member_type
	, user_contract.residence_province
	, user_contract.birthday
	, extract('year' from age(post_submitted.timestamp, user_contract.birthday)) as age
from post_submitted
inner join episodes
	on post_submitted.episode_id = episodes.episode_id
inner join user_contract
	on episodes.patient_id = user_contract.user_id
		and post_submitted.timestamp <@ user_contract.during
{% if target.name == 'dev' %}
  where timestamp > current_date - interval '1 months'
{% endif %}
