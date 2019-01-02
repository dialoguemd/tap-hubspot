with question_replied as (
        select * from {{ ref('countdown_question_replied') }}
    )

select
    episode_id
    , max(case
        when reply_labels = '["someone_else"]' then null
        else reply_values::json->>0
      end)
    as episode_subject
    , max(replied_at) as timestamp
    , max(date_trunc('week', replied_at)) as date_week
from question_replied
where question_name = 'episode_subject'
group by 1
