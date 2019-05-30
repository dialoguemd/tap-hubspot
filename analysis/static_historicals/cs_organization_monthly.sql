CREATE TABLE
        static_historicals.cs_organization_monthly
    AS (
    
    with
        data as (
            select * from analytics.cs_organization_monthly
            where date_month < date_trunc('month', current_timestamp)
        )
        
    select date_month
        , account_name
        , sum(total_daus) as total_daus_sum
        , sum(total_active_on_chat) as total_active_on_chat_sum
        , sum(total_active_on_video) as total_active_on_video_sum
        , sum(employee_invited_count) as employee_invited_count_sum
        , sum(employee_signed_up_count) as employee_signed_up_count_sum
        , sum(employee_activated_count) as employee_activated_count_sum
        , sum(dependent_invited_count) as dependent_invited_count_sum
        , sum(dependent_signed_up_count) as dependent_signed_up_count_sum
        , sum(dependent_activated_count) as dependent_activated_count_sum
        , sum(child_invited_count) as child_invited_count_sum
        , sum(child_signed_up_count) as child_signed_up_count_sum
        , sum(child_activated_count) as child_activated_count_sum
        , current_timestamp as archived_at
    from data
    group by 1,2
)
