with
    qnaire_completed as (
        select * from {{ ref('countdown_qnaire_completed') }}
    )

    , question_replied as (
        select * from {{ ref('countdown_question_replied') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , completed as (
        select * from qnaire_completed
        where qnaire = 'phq9'
    )

    , replies as (
        select qnaire_tid
            , question_tid
            , replace(replace(reply_values, '[', ''), ']', '') :: integer as reply_values
        from question_replied
        where qnaire_name = 'phq9'
            and reply_values <> '[null]'
    )
    
    , qnaires as (
        select qnaire_tid
            , sum(reply_values) as score
        from replies
        group by 1
    )

select episodes.patient_id as user_id
    , episodes.episode_id
    , qnaires.qnaire_tid
    , completed.timestamp
    , first_value(completed.timestamp)
                over (partition by episodes.user_id
                    order by completed.timestamp) as first_phq9_timestamp
    , qnaires.score
    , coalesce(
        extract(epoch from completed.timestamp -
            lag(completed.timestamp)
                over (partition by episodes.user_id
                    order by completed.timestamp)
        ) / 86400, 0) as days_since_most_recent_phq9
    , coalesce(
        extract(epoch from completed.timestamp -
            first_value(completed.timestamp)
                over (partition by episodes.user_id
                    order by completed.timestamp)
        ) / 86400, 0) as days_since_first_phq9
    , row_number()
        over (partition by episodes.user_id order by completed.timestamp)
        as rank
    , coalesce(
        case when qnaires.score = 0 then 0
        else (qnaires.score - (lag(qnaires.score)
        over (partition by episodes.user_id order by completed.timestamp)))
        * 1.0 / qnaires.score
        end, 0) as difference_from_most_recent_score
    , coalesce(
        case when qnaires.score = 0 then null
        else (qnaires.score - (first_value(qnaires.score)
        over (partition by episodes.user_id order by completed.timestamp)))
        * 1.0 / qnaires.score
        end, 0) as difference_from_first_score
from completed
inner join episodes
    using (episode_id)
left join qnaires
    using (qnaire_tid)
