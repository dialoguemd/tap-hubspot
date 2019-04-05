with
	active_users as (
		select * from {{ ref('active_users')}}
	)

	, organization_days as (
		select * from {{ ref('client_organization_days')}}
	)

	, consults_daily as (
		select date_day
			, organization_id
			, count(distinct dau_id)
				filter(
					where (((active_on_video_gp or active_on_video_unidentified)
							and date_month >= '2017-11-01')
						or (active_on_video and date_month < '2017-11-01'))
					) +
				count(distinct dau_id)
					filter(where active_on_chat)
				as total_consults
		from active_users
		group by 1,2
	)

select organization_days.date_day
	, organization_days.organization_id
	, consults_daily.total_consults
	, sum(consults_daily.total_consults) over (
            partition by consults_daily.organization_id
            order by consults_daily.date_day)
        as total_consults_cum
from organization_days
left join consults_daily
	using (organization_id, date_day)
