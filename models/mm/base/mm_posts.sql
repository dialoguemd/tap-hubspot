select id as post_id
   , type as post_type
   , user_id
   , mm_user_id
   , user_type
   , channel_id as episode_id
   , created_at
   , message_length
   , question_mark_count as count_question_marks
   , next_appointment_time::timestamp as next_appointment
   , includes_question_mark as is_question
   , type != '' as is_internal_post
   , mention
from mm.posts
where
{% if target.name == 'dev' %}
    created_at > '2018-08-01 00:00:00.000+00' and
{% endif %}
    created_at < '2018-09-01 00:00:00.000+00'
