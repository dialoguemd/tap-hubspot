with
    wiw_shifts as (
        select * from {{ ref('wiw_shifts_detailed') }}
    )

    , reduced_hours as (
        select * from {{ ref('dimension_careplatform_reduced_hours') }}
    )

    , dates as (
        select generate_series(
            '2016-09-08'::timestamp
            , current_date + interval '30 days'
            , interval '1 day'
        ) as date_day
    )

    , dates_regular_hours as (
        select dates.date_day
            , extract('dow' from dates.date_day) as date_dow
        from dates
        left join reduced_hours
            using (date_day)
        where reduced_hours.date_day is null
    )

    , opening_hours as (
        select date_day
            , date_day + '10:00:00' as opening_hour_est
            , date_day + '16:00:00' as closing_hour_est
        from dates_regular_hours
        where (
                date_dow = 0
                and date_day > '2017-11-01'
            )
            or date_dow = 6
        union all
        select date_day
            , date_day + '08:00:00' as opening_hour_est
            , date_day + '20:00:00' as closing_hour_est
        from dates_regular_hours
        where date_dow in (1,2,3,4)
            or (
                date_dow = 5
                and date_day > '2017-08-01'
            )
        union all
        select date_day
            , date_day + '08:00:00' as opening_hour_est
            , date_day + '17:00:00' as closing_hour_est
        from dates_regular_hours
        where date_dow = 5
            and date_day < '2017-08-01'
        union all
        select date_day
            , date_day + '10:00:00' as opening_hour_est
            , date_day + '16:00:00' as closing_hour_est
        from reduced_hours
    )

    , shifts as (
        select date_trunc('day', start_time at time zone 'America/Montreal') as date
            , min(start_time_est) as shift_start_time_est
            , max(end_time_est) as shift_end_time_est
            , tsrange(min(start_time_est), max(end_time_est)) as shift_span_est
        from wiw_shifts
        where location_name = 'Virtual Care Platform'
        group by 1
    )

select opening_hours.date_day
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
    on opening_hours.date_day = shifts.date
where opening_hours.date_day not in (
    '2018-01-01'
    , '2017-12-31'
    , '2017-12-30'
    , '2017-12-25'
    , '2017-12-24'
)
