CREATE TABLE
        static_historicals.client_dashboard_daily
    AS (
    
    with
        data as (
            select * from analytics.client_dashboard_daily
        )
        
    select date_day
        , account_name
        , sum(median_age) as median_age_sum
        , sum(invited_employee_count) as invited_employee_count_sum
        , sum(signed_up_employee_rate) as signed_up_employee_rate_sum
        , sum(signed_up_employee_count) as signed_up_employee_count_sum
        , sum(signed_up_family_member_count) as signed_up_family_member_count_sum
        , sum(activated_employee_count) as activated_employee_count_sum
        , sum(activated_family_member_count) as activated_family_member_count_sum
        , sum(survey_count_cum) as survey_count_cum_sum
        , sum(survey_sum_cum) as survey_sum_cum_sum
        , sum(total_consults) as total_consults_sum
        , current_timestamp as archived_at
    from data
    group by 1,2
)
