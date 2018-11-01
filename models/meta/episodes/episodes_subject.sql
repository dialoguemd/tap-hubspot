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
    from question_replied
    where qnaire_name = 'top_level_greeting'
        and question_name = 'episode_subject'
    group by 1
