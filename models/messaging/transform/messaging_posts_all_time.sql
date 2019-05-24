
{{
  config(
    materialized='incremental',
    unique_key='post_id',
    post_hook=[
       "{{ postgres.index(this, 'post_id')}}",
    ]
  )
}}

with
	messaging_posts as (
		select * from {{ ref('messaging_posts') }}
		-- Add incremental jinja to messaging_posts to only pull recent posts
		{% if is_incremental() %}
		where created_at > (select max(created_at) from {{ this }})
		{% endif %}
	)

	, mm_posts as (
		select * from {{ ref('mm_posts') }}
		-- Add incremental jinja to mm_posts to effectively ignore mm_posts if incremental
		-- mm_posts has been deprecated since 2018-09-01
		{% if is_incremental() %}
		where created_at > (select max(created_at) from {{ this }})
		{% endif %}
	)

	, unioned as (
		select *
		from messaging_posts

		union all

		select *
		from mm_posts
	)

select post_id
	, post_type
	, user_id
	, mm_user_id
	-- Add a case for users created before 2017-03-27 who have no user_type
	, case 
		when user_type is null 
			and post_type is null
			and user_id <> 'zy4q8gkk7bn67f6q7345qwztyo'
		then 'patient'
		else user_type
		end as user_type
	, episode_id
	, message_length
	, count_question_marks
	, next_appointment
	, is_question
	, is_internal_post
	, mention
	-- Take the max to take the later post and ignore Segment's manipulation
	-- of timestamps; take the later to not interfere also with the incremental
	-- materialization of the model
	, max(created_at) as created_at
	, date_trunc('day', max(created_at)) as created_at_day
	, max(timezone('America/Montreal', created_at)) as created_at_est
	, date_trunc('day',
	max(timezone('America/Montreal', created_at))
	) as created_at_day_est
from unioned
{{ exclude_test_users() }}
{{ dbt_utils.group_by(12) }}
