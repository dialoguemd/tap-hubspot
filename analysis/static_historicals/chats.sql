CREATE TABLE
        static_historicals.chats
    AS (
    
    with
        data as (
            select * from analytics.chats
            where date_week < date_trunc('week', current_timestamp)
        )
        
    select date_week
        , chat_type
        , count(distinct user_id) as user_count
        , count(distinct episode_id) as episode_count
        , count(distinct patient_id) as patient_count
        , current_timestamp as archived_at
    from data
    group by 1,2
)
