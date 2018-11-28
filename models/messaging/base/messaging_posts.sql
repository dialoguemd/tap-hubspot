select post_id
    , post_type
    , user_id
    , mm_user_id
    , user_type
    , channel_id as episode_id
    , timestamp as created_at
    , message_length
    , count_question_marks
    , next_appointment::timestamp
    , is_question
    , is_internal_post
    , mention
from messaging.posts
where
{% if target.name == 'dev' %}
    timestamp > current_date - interval '1 months'
{% else %}
-- posts data source updated as of September 2018
    timestamp >= '2018-09-01 00:00:00.000+00'
{% endif %}
