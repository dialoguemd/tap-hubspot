with
    posts as (
        select * from {{ ref('messaging_posts_w_lag_detail') }}
    )

select user_id
   , post_id
   , episode_id
   , date_day
   , created_at
   , previous_post_at
   , timezone('America/Montreal', created_at) as created_at_est
   , extract(epoch from age(created_at, previous_post_at))/60 as in_chat_time
from posts
where user_type = 'physician'
   and previous_user_type = 'patient'
   and date_day = date_trunc('day', previous_post_at)
