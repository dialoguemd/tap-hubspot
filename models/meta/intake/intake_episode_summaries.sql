with 
    events as (
        select * from {{ ref('intake_events_by_episode') }}
    )

    , ordered as (
        select episode_id
            , min(reason_for_visit) as reason_for_visit
            , min(issue_type) as issue_type
            , min(cc_code) as cc_code
            , min(outcome) as outcome
            , count(*) as count_events
            , bool_or(dxa_rank is not null) as has_dxa
            , bool_or(type = 'free_text') as has_interruption

            -- Jinja loop for iterating through events 1 to 15
            {% for n in range(1,16) %}

            , max(event_name) filter (where rank = {{n}}) as event_{{n}}

            {% endfor %}

        from events
        group by 1
    )

select *
    , case when has_dxa
        and not has_interruption
        then 'happy_path'
      when has_interruption
        and has_dxa
        then 'interrupted_by_free_text'
      when outcome is null
        then 'not_resolved'
      else 'no_dxa'
      end as intake_type
from ordered
