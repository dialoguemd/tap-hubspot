with
   posts as (
		select * from {{ ref('messaging_posts_all_time') }}
   )

select date_trunc('day', timezone('America/Montreal', created_at)) as date_day
	, user_id
	, episode_id
	, count(*) as post_count
	, min(created_at) as first_message_created_at
from posts
where not is_internal_post
	and user_type = 'patient'
group by 1,2,3
