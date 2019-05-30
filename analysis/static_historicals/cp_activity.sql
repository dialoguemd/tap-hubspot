-- This table takes too long to build so it is currently not in use

CREATE TABLE
        static_historicals.cp_activity
    AS (
    
    with
        data as (
            select * from analytics.cp_activity
        )
        
    select date
        , episode_id
        , count(distinct shift_id) as shift_count
        , count(distinct cp_activity_id) as cp_activity_count
        , sum(time_spent) as time_spent_sum
        , current_timestamp as archived_at
    from data
    group by 1,2
)
