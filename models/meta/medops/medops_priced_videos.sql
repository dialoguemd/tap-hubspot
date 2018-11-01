with cp_activity_detailed as (
        select * from {{ ref( 'pdt_cp_activity_detailed' ) }}
    )

    , monthly_video_cost as (
        select * from {{ ref( 'medops_video_cost_monthly' ) }}
    )

	select episode_id
        , date_trunc('day', date) as date
        , count(*) filter(where issue_type = 'psy') * per_video_cost * 2 as gp_psy_cost
        , count(*) filter(where issue_type <> 'psy') * per_video_cost as gp_other_cost
	from cp_activity_detailed
	left join monthly_video_cost
		on date_trunc('month', cp_activity_detailed.date) = monthly_video_cost.month
	where cp_activity_detailed.main_specialization = 'Family Physician'
		and activity = 'video'
		and is_active
		and time_spent > 60
	group by 1,2,per_video_cost
	