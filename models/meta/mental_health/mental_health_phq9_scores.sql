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
        where qnaire_name = 'phq9'
    )

    , replies as (
        select qnaire_tid
            , question_tid
            , qnaire_name
            , user_id
            , timestamp
            , replace(replace(reply_values, '[', ''), ']', '') :: integer as reply_values
        from question_replied
        where qnaire_name in ('phq9','gad7')
            and question_name not in ('phq_9_q11', 'gad_7_q8')
            and reply_values <> '[null]'
    )
    
    , phq as (
        select qnaire_tid
            , sum(reply_values) as score
            , count(*) as questions_asked_count_phq
        from replies
        where qnaire_name = 'phq9'
        group by 1
    )

    , gad_questions as (
        select user_id
            , qnaire_tid
            , min(timestamp) as timestamp
            , sum(reply_values) as score
            , count(*) as questions_asked_count_gad
        from replies
        where qnaire_name = 'gad7'
        group by 1,2
    )

    , gad as (
        select user_id
            , qnaire_tid
            , score
            , questions_asked_count_gad
            , row_number() over (partition by user_id order by timestamp) as rank
        from gad_questions
    )

select episodes.patient_id as user_id
    , episodes.episode_id
    , episodes.issue_type
    , phq.qnaire_tid
    , completed.timestamp
    , first_value(completed.timestamp)
                over (partition by episodes.user_id
                    order by completed.timestamp) as first_phq9_timestamp
    , phq.score
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
        over (partition by episodes.user_id, episodes.issue_type order by completed.timestamp)
        as rank
    , coalesce(
        case when phq.score = 0 then 0
        else (phq.score - (lag(phq.score)
        over (partition by episodes.user_id order by completed.timestamp)))
        * 1.0 / phq.score
        end, 0) as difference_from_most_recent_score
    , coalesce(
        case when phq.score = 0 then null
        else (phq.score - (first_value(phq.score)
        over (partition by episodes.user_id order by completed.timestamp)))
        * 1.0 / phq.score
        end, 0) as difference_from_first_score
    , gad.score as initial_gad7_score
from completed
inner join episodes
    using (episode_id)
inner join phq
    using (qnaire_tid)
left join gad
    on completed.user_id = gad.user_id
    -- Only include the initial score
    and gad.rank = 1
    -- And only if it was completed
    and gad.questions_asked_count_gad = 7
where phq.questions_asked_count_phq = 9
