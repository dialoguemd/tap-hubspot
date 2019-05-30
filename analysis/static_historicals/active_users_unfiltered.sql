CREATE TABLE
        static_historicals.active_users_unfiltered
    AS (
    
    with
        data as (
            select * from analytics.active_users_unfiltered
            where date_day < date_trunc('day', current_timestamp)
        )
        
    select date_day
        , family_member_type
        , count(distinct account_id) as account_count
        , count(distinct organization_id) as organization_count
        , count(distinct patient_id) as patient_count
        , current_timestamp as archived_at
    from data
    group by 1,2
)
