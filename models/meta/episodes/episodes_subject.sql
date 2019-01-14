with question_replied as (
        select * from {{ ref('countdown_question_replied') }}
    )

    , channels as (
        select * from {{ ref('messaging_channels') }}
    )

select
    -- Use channels to ensure all channels have a record
    channels.episode_id
    -- Check episode_subject answer and if not available coealesce to user
    , coalesce(
        max(case
            when question_replied.reply_labels = '["someone_else"]' then null
            else question_replied.reply_values::json->>0
          end),
        max(channels.user_id))
    as episode_subject
    , max(question_replied.replied_at) as timestamp
    , max(date_trunc('week', question_replied.replied_at)) as date_week
from channels
left join question_replied
    using (episode_id)
where question_name = 'episode_subject'
group by 1
