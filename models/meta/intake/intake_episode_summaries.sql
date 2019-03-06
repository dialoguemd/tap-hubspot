with 
    events as (
        select * from {{ ref('intake_events_by_episode') }}
    )

    , ordered as (
        select episode_id
            , min(timestamp) as intake_started_at
            , min(reason_for_visit) as reason_for_visit
            , min(issue_type) as issue_type
            , min(cc_code) as cc_code
            , min(outcome) as outcome
            , count(*) as count_events
            , bool_or(dxa_rank is not null) as has_dxa
            , bool_or(apt_booking_rank is not null) as has_apt_booking
            , bool_or(resolved_rank is not null) as has_resolved
            , bool_or(type = 'free_text') as has_interruption

            , extract(epoch from
                min(timestamp) filter (where type = 'appointment_booking')
                - coalesce(
                    min(timestamp) filter (where event_name = 'dxa'),
                    min(first_message_nurse))
                )/60
                as time_to_apt_booking_dxa_mins

            , extract(epoch from
                min(timestamp) filter (where type = 'resolved')
                - coalesce(
                    min(timestamp) filter (where event_name = 'dxa'),
                    min(first_message_nurse))
                )/60
                as time_to_resolve_dxa_mins

            {% for type in ["total", "cc", "nc"] %}

            , sum(time_spent_{{type}})
                filter (where rank between dxa_rank and apt_booking_rank)/60
                as active_time_to_apt_booking_dxa_mins_{{type}}
            , sum(time_spent_{{type}})
                filter (where rank between dxa_rank and resolved_rank)/60
                as active_time_to_resolved_dxa_mins_{{type}}

            {% endfor %}

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
