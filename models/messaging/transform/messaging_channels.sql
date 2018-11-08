with posts_all_time as (
    select * from {{ ref('messaging_posts_all_time') }}
)

select episode_id
    , min(created_at) as created_at
    , max(created_at) as updated_at
    , null::timestamptz as deleted_at
    , max(created_at) filter(where user_type in ('physician', 'patient')) as last_post_at
    , min(created_at) filter(where user_type = 'physician') as first_message_care_team
    , min(created_at) filter(where user_type = 'patient') as first_message_patient
    , min(user_id) filter(where user_type = 'patient') as user_id
    , count(*) filter(where user_type in ('physician', 'patient')) as count_messages
from posts_all_time
group by 1
