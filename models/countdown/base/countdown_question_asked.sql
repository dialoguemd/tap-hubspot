with
    asked as (
        select cd_q_tid as question_tid
            , cd_qnaire_tid as qnaire_tid
            , timestamp
            , user_id
            , channel_id as episode_id
            , cd_qnaire as qnaire_name
            , cd_q_id as question_name
            , row_number() over (partition by cd_q_tid order by timestamp) as rank
        from countdown.question_ask
    )

select question_tid
    , qnaire_tid
    , timestamp
    , user_id
    , episode_id
    , qnaire_name
    , question_name
from asked
where rank = 1
