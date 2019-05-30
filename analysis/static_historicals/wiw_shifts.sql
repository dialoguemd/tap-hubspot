CREATE TABLE
        static_historicals.wiw_shifts
    AS (
    
    with
        data as (
            select * from analytics.wiw_shifts
            where start_day_est < current_timestamp
        )
        
    select start_day_est
        , count(distinct shift_id) as shift_count
        , count(distinct wiw_user_id) as wiw_user_count
        , count(distinct position_id) as position_count
        , count(distinct location_id) as location_count
        , sum(hours) as hours_sum
        , current_timestamp as archived_at
    from data
    group by 1
)
