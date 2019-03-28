with
    chats_out_of_hours_slas as (
        select * from {{ ref('chats_out_of_hours_slas') }}
    )

    , aggregate as (
        select date_trunc('week', date_day_est) as date_week
            , 1.0 * count(*) filter(where is_answered_next_day)
                / count(*) as answered_next_day_rate
            , 1.0 * count(*) filter(where is_answered_within_3_opened_hours)
                / count(*) as answered_within_3_opened_hours_rate
            , count(*) as chats_count
            , count(*) filter(where not is_answered_next_day)
                as chats_not_answered_next_day_count
            , count(*) filter(where not is_answered_next_day)
                as chats_not_answered_within_3_hours_count
        from chats_out_of_hours_slas
        where date_day_est > '2018-02-01'
            and date_day_est < current_date - interval '1 day'
        group by 1
    )

select *
from aggregate
where (answered_next_day_rate < .93
    or answered_within_3_opened_hours_rate < .93)
    and date_trunc('week', current_date) > date_week
