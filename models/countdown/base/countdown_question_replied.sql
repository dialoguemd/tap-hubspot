with
    replies as (
        select cd_q_tid as question_tid
            , cd_qnaire_tid as qnaire_tid
            , timestamp as replied_at
            , user_id
            , channel_id as episode_id
            , cd_qnaire as qnaire_name
            , cd_q_id as question_name
            , quetion_reply_items as reply_labels
            , quetion_reply_value as reply_value
            , quetion_reply_values as reply_values
            , case when cd_q_id like '%q1' or cd_q_id like '%q2' then 'Depression'
                 when cd_q_id like '%q3' or cd_q_id like '%q4' then 'Anxiety'
                 when cd_q_id like '%q5' or cd_q_id like '%q6'
                    or cd_q_id like '%q7' or cd_q_id like '%q8' then 'Stress'
                 else 'No Category'
                 end as question_category
            , row_number() over (partition by cd_q_tid order by timestamp) as rank
        from countdown.question_reply
    )

select question_tid
    , qnaire_tid
    , replied_at
    , user_id
    , episode_id
    , qnaire_name
    , question_name
    , reply_labels
    , reply_value
    , reply_values
    , question_category
from replies
where rank = 1
