with
	practitioners as (
		select * from {{ ref('practitioners') }}
	)

	, chats_all_time as (
		select * from {{ ref('chats') }}
	)

	, posts as (
		select * from {{ ref('messaging_posts_all_time') }}
	)

select practitioners.user_name
	, practitioners.main_specialization
	, chats_all_time.created_at_day as date_day
	, chats_all_time.url_zorro
	, chats_all_time.user_id as patient_id
	, chats_all_time.chat_type
from chats_all_time
inner join posts
	using (episode_id)
inner join practitioners
	on posts.user_id = practitioners.user_id
where chats_all_time.created_at_day > (current_date - interval '90 days')
	and not posts.is_internal_post
group by 1,2,3,4,5,6
