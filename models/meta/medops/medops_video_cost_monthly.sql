with monthly_count_videos as (
    	select * from {{ ref( 'medops_count_videos_monthly' ) }}
    )

	, fl_cost as (
    	select * from {{ ref( 'medops_fl_costs_by_main_spec' ) }}
    )

    select month
        , fl_gp_cost / (other_video_count + 2*psy_video_count) as per_video_cost
    from monthly_count_videos
    left join fl_cost using (month)
