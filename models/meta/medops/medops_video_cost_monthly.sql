with monthly_count_videos as (
    	select * from {{ ref( 'medops_count_videos_monthly' ) }}
    )

	, fl_cost as (
    	select * from {{ ref( 'medops_fl_costs_by_main_spec' ) }}
    )

    select monthly_count_videos.month
        , coalesce(fl_cost.fl_gp_cost
            / (monthly_count_videos.other_video_count
                + 2*monthly_count_videos.psy_video_count),0)
        as per_video_cost
    from monthly_count_videos
    left join fl_cost on monthly_count_videos.month = fl_cost.date_month
