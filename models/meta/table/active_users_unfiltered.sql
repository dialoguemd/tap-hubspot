with
	chats as (
		select * from {{ ref('chats') }}
	)

	, videos as (
		select * from {{ ref('videos_daily') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, active_daily as (
		select coalesce(chats.date_day_est, videos.date_day_est) as date_day
			, coalesce(chats.patient_id, videos.patient_id)  as patient_id
			, least(chats.first_message_created_at
				, videos.first_timestamp
			) as first_activity_created_at
			, chats.first_set_active is not null as set_active
			, chats.patient_id is not null as active_on_chat
			, videos.patient_id is not null as active_on_video
			, coalesce(videos.includes_video_gp, false) as active_on_video_gp
			, coalesce(videos.includes_video_np, false) as active_on_video_np
			, coalesce(videos.includes_video_nc, false) as active_on_video_nc
			, coalesce(videos.includes_video_cc, false) as active_on_video_cc
			, coalesce(videos.includes_video_unidentified, false) as active_on_video_unidentified
		from chats
		full outer join videos
			using (date_day_est, patient_id)
	)

select active_daily.*
	, date_trunc('week', active_daily.date_day) as date_week
	, date_trunc('month', active_daily.date_day) as date_month
	, date_trunc('year', active_daily.date_day) as date_year
	, 'D:' || active_daily.date_day || 'U:' || active_daily.patient_id as dau_id
	, active_daily.patient_id as user_id
	, user_contract.is_employee as is_employee
	, user_contract.family_member_type
	, user_contract.language
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.country
	, user_contract.residence_province
	, extract('year' from age(active_daily.date_day, user_contract.birthday)) as age
	, user_contract.gender
	, case
		when active_daily.date_day < user_contract.billing_start_date
		then 'ooc'
		when not user_contract.organization_is_paid
		then 'unpaid'
		else 'paid'
	end as contract_status
from active_daily
inner join user_contract
	on active_daily.patient_id = user_contract.user_id
	and active_daily.first_activity_created_at <@ user_contract.during_est
