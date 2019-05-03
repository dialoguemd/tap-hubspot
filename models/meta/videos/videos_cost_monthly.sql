with monthly_count_videos as (
        select * from {{ ref('videos_monthly') }}
    )

    , fl_cost as (
        select * from {{ ref('finance_revenue_and_costs_monthly') }}
    )

select monthly_count_videos.date_month
    , coalesce(
        fl_cost.fl_gp_cost
            / (
                monthly_count_videos.other_video_count
                + 2 * monthly_count_videos.psy_video_count
            )
        -- default cost per video
        , 45 * (
            monthly_count_videos.other_video_count
            + 2 * monthly_count_videos.psy_video_count
        ) / (
            monthly_count_videos.other_video_count
            + monthly_count_videos.psy_video_count
        )
    )
    as per_video_cost
from monthly_count_videos
left join fl_cost
	using (date_month)
