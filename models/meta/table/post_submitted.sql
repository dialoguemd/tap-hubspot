
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    post_hook=[
       "{{ postgres.index(this, 'event_id')}}",
    ]
  )
}}

-- This model is for identifying context and demographics of active patient
-- users. This could be combined with an active users / chats table to be more
-- efficient.

-- TODO redesign chats, active users, and post-submitted to eliminate overlap

with
	post_submitted as (
		select * from {{ ref('patientapp_submit_post_success') }}
		where rank = 1
		{% if is_incremental() %}
			and timestamp > (select max(timestamp) from {{ this }})
		{% endif %}
	)

	, episodes as (
		select * from {{ ref('episodes_with_contracts') }}
	)

select post_submitted.*
	, episodes.patient_id
	, episodes.account_id
	, episodes.account_name
	, episodes.organization_id
	, episodes.organization_name
	, episodes.gender
	, episodes.language
	, episodes.family_member_type
	, episodes.residence_province
	, episodes.age
	, episodes.contract_id
from post_submitted
inner join episodes
	using (episode_id)
{% if target.name == 'dev' %}
  where timestamp > current_date - interval '2 months'
{% endif %}
