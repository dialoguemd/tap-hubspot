with
	posts as (
		select * from {{ ref('messaging_posts_patient_daily') }}
	)

	, videos as (
		select * from {{ ref('videos_daily') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, posts_patients as (
		select posts.date_day
			, coalesce(episodes_subject.episode_subject, posts.user_id) as patient_id
			, min(posts.first_message_created_at) as first_message_created_at
		from posts
		left join episodes_subject
			using (episode_id)
		group by 1,2
	)

	, active_daily as (
		select coalesce(posts_patients.date_day, videos.date_day) as date_day
			, coalesce(posts_patients.patient_id, videos.patient_id)  as patient_id
			, least(posts_patients.first_message_created_at
				, videos.first_timestamp
			) as first_activity_created_at
			, posts_patients.patient_id is not null as active_on_chat
			, videos.patient_id is not null as active_on_video
			, coalesce(videos.includes_video_gp, false) as active_on_video_gp
			, coalesce(videos.includes_video_np, false) as active_on_video_np
			, coalesce(videos.includes_video_nc, false) as active_on_video_nc
			, coalesce(videos.includes_video_cc, false) as active_on_video_cc
			, coalesce(videos.includes_video_unidentified, false) as active_on_video_unidentified
		from posts_patients
		full outer join videos
			using (date_day, patient_id)
	)

select active_daily.*
	, date_trunc('week', active_daily.date_day) as date_week
	, date_trunc('month', active_daily.date_day) as date_month
	, date_trunc('year', active_daily.date_day) as date_year
	, 'D:' || active_daily.date_day || 'U:' || active_daily.patient_id as dau_id
	, active_daily.patient_id as user_id
	, user_contract.is_employee as is_employee
	, case
		when user_contract.is_employee
		then 'Employee'
		else user_contract.family_member_type
	end as family_member_type
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
	and active_daily.first_activity_created_at <@ user_contract.during
