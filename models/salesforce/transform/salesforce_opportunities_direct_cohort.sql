with
    opportunities as (
        select *
        from {{ ref('salesforce_opportunities_detailed_direct') }}
    )

    , opps_clean as (
        select *
            , generate_series(
                meeting_date
                , current_date - interval '1 month'
                , interval '1 month'
            ) as month_start
        from opportunities
    )

select *
    , coalesce(close_won_date < month_start + '1 month', false) as is_closed_won_cohort
    , row_number()
        over(
            partition by opportunity_id order by month_start
        ) as month_since_first_meeting
from opps_clean
where month_start < current_date - interval '1 month'
