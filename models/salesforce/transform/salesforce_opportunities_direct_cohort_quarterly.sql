with
    opportunities as (
        select * from {{ ref('salesforce_opportunities_detailed_direct') }}
    )

    , opps_clean as (
        select *
            , generate_series(
                meeting_date
                , current_date - interval '3 months'
                , interval '3 months'
            ) as quarter_start
        from opportunities
    )

select *
    , coalesce(close_won_date < quarter_start + interval '3 months', false) as is_closed_won_cohort
    , row_number()
        over(
            partition by opportunity_id order by quarter_start
        ) as quarters_since_first_meeting
from opps_clean
where quarter_start < current_date - interval '3 months'
