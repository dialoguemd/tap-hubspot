with
    questions as (
        select * from {{ ref('dxa_questions') }}
    )
    
    , completion_stats_tmp as (
        select * from {{ ref('countdown_qnaire_completion_stats') }}
    )

    , completion_stats as (
        select started_at
            , episode_id
            , user_id
            , completed_at
            , qnaire_tid
            , questionnaire_completed
            , questionnaire_completion_time
        from completion_stats_tmp
        where qnaire_name = 'dxa'
    )

    , respondent_type as (
        select qnaire_tid
            , rank()
                over (partition by user_id order by completed_at)
                as dxa_rank
        from completion_stats
    )

select questions.qnaire_tid
    , questions.episode_id
    , completion_stats.user_id
    , completion_stats.started_at
    , completion_stats.completed_at
    , case when respondent_type.dxa_rank = 1 then 'first_dxa'
        else 'repeat'
        end as respondent_type
    , questions.age
    , questions.gender
    , questions.language
    , questions.outcome
    , questions.issue_type
    , questions.cc_code
    , questions.reason_for_visit
    , questions.cc_label_en
    , completion_stats.questionnaire_completed
    , count(questions.question_tid) as questions_asked_count
    , bool_or(questions.response_type = 'interrupted')
        as drop_off_free_text
    , case when bool_or(questionnaire_completed) then 'completed'
        when bool_or(questions.response_type = 'interrupted') then 'interrupted'
        else 'error' end
        as completion_type
    , coalesce(
        min(completion_stats.questionnaire_completion_time),
        extract(epoch from max(questions.replied_at) - min(questions.asked_at))
        ) as completion_time
from questions
inner join completion_stats
    using (qnaire_tid, episode_id)
inner join respondent_type
    using (qnaire_tid)
{{ dbt_utils.group_by(15) }}
