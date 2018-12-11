with
	active_users as (
		select * from {{ ref('active_users')}}
	)

select date_month
	, organization_id

	, count(distinct dau_id) as total_daus
	, count(distinct user_id) as total_active_users
	, count(distinct dau_id)
		filter(where active_on_chat) as total_active_on_chat
	, count(distinct dau_id)
		filter(where active_on_video) as total_active_on_video
	, count(distinct dau_id)
		filter(where
			((active_on_video_gp or active_on_video_unidentified)
			and date_month >= '2017-11-01')
			or (active_on_video and date_month < '2017-11-01')
			) as total_active_on_video_gp

	-- Jinja loop for family member types
	{% for family_member_type in ["Employee", "Dependent", "Child"] %}

	-- Count of active users is count active in given month
	, count(distinct user_id)
		filter(where family_member_type = '{{family_member_type}}')
			as total_active_users_{{family_member_type}}

	-- Theses counts are in DAUs and will be summed cumulatively later
	, count(distinct dau_id)
		filter(where family_member_type = '{{family_member_type}}')
			as total_daus_{{family_member_type}}
	, count(distinct dau_id) 
		filter(
			where active_on_chat and family_member_type = '{{family_member_type}}'
			) as total_active_on_chat_{{family_member_type}}
	, count(distinct dau_id)
		filter(
			where active_on_video and family_member_type = '{{family_member_type}}'
			) as total_active_on_video_{{family_member_type}}
	, count(distinct dau_id)
		filter(
			where family_member_type = '{{family_member_type}}'
				and (((active_on_video_gp or active_on_video_unidentified)
					and date_month >= '2017-11-01')
				or (active_on_video and date_month < '2017-11-01'))
			) as total_active_on_video_gp_{{family_member_type}}

  {% endfor %}

from active_users
group by 1,2
