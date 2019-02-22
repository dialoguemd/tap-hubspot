with
	chats as (
		select * from {{ ref('chats') }}
	)

    , counted as (
        select episode_id
            , count(*) filter (where chat_type = 'New Episode') as new_episode_count
        from chats
        group by 1
    )

select *
from counted
where new_episode_count > 1
