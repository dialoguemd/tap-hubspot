with
    posts as (
        select * from {{ ref('messaging_posts_all_time') }}
    )

select user_id
    , post_id
    , date_trunc('day', created_at) as date_day
    , episode_id
    , user_type
    , created_at
    , lag(created_at) over (partition by episode_id order by created_at)
      as previous_post_at
    , lag(user_type) over (partition by episode_id order by created_at)
      as previous_user_type
from posts
where user_id is not null
   and not is_internal_post
