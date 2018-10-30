with wiw_shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , dates as (
        select generate_series('2016-09-08'::timestamp, current_date + interval '30 days', interval '1 day') as date
    )

    , opening_hours as (
        select date
            , date + '10:00:00' as opening_hour_est
            , date + '16:00:00' as closing_hour_est
        from dates
        where (
                extract('dow' from date) = 0
                and date > '2017-11-01'
            )
            or (
                extract('dow' from date) = 6
            )
        union all
        select date
            , date + '08:00:00' as opening_hour_est
            , date + '20:00:00' as closing_hour_est
        from dates
        where extract('dow' from date) in (1,2,3,4)
            or (
                extract('dow' from date) = 5
                and date > '2017-08-01'
            )
        union all
        select date
            , date + '08:00:00' as opening_hour_est
            , date + '17:00:00' as closing_hour_est
        from dates
        where extract('dow' from date) = 5
            and date < '2017-08-01'
    )

    , shifts as (
          select date_trunc('day', start_time at time zone 'America/Montreal') as date
        , min(start_time at time zone 'America/Montreal') as shift_start_time_est
        , max(end_time at time zone 'America/Montreal') as shift_end_time_est
        , tsrange(min(start_time at time zone 'America/Montreal'), max(end_time at time zone 'America/Montreal')) as shift_span_est
      from wiw_shifts
      where location_name = 'Virtual Care Platform'
      group by 1
    )

    select opening_hours.date
        , opening_hours.opening_hour_est
        , opening_hours.closing_hour_est
        , tsrange(
            opening_hours.opening_hour_est,
            opening_hours.closing_hour_est
        ) as opening_span_est
        , shifts.shift_start_time_est
        , shifts.shift_end_time_est
        , shifts.shift_span_est
    from opening_hours
    left join shifts
        on opening_hours.date = shifts.date
    where opening_hours.date not in (
        '2018-01-01'
        , '2017-12-31'
        , '2017-12-30'
        , '2017-12-25'
        , '2017-12-24'
    )
