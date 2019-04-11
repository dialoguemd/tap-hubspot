
{{ config(materialized='table') }}

with
	posts_all_time as (
		select * from {{ ref('messaging_posts_all_time') }}
	)

	, tmp as (
		select episode_id
			, min(created_at) as created_at
			, max(created_at) as updated_at
			, null::timestamptz as deleted_at
			, max(created_at) filter(where user_type in ('physician', 'patient'))
				as last_post_at
			, min(user_id) filter(where user_type = 'patient') as user_id
			, count(*) filter(where user_type in ('physician', 'patient'))
				as count_messages
		from posts_all_time
		group by 1
		having min(user_id) filter(where user_type = 'patient') is not null
	)

select *
	, 'https://zorro.dialogue.co/conversations/' || episode_id
		as url_zorro
	, deleted_at <> '1970-01-01T00:00:00.000Z' as is_deleted
from tmp
