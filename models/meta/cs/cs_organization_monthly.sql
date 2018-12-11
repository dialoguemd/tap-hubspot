with
	usage as (
        select * from {{ ref('cs_usage_by_org_monthly')}}
    )

    , activity as (
        select * from {{ ref('cs_activity_by_org_monthly')}}
    )

    , activity_cum as (
        select * from {{ ref('cs_activity_cum_by_org_monthly')}}
    )

select usage.date_month
	, usage.organization_name
	, usage.organization_id
	, usage.account_name

	, coalesce(activity.total_daus,0) as total_daus
	, coalesce(activity.total_active_on_chat,0) as total_active_on_chat
	, coalesce(activity.total_active_on_video,0) as total_active_on_video
	, coalesce(activity.total_active_on_video_gp,0) as total_active_on_video_gp

	, coalesce(activity_cum.total_daus_cum,0)
		as total_daus_cum
	, coalesce(activity_cum.total_active_on_chat_cum,0)
		as total_active_on_chat_cum
	, coalesce(activity_cum.total_active_on_video_cum,0)
		as total_active_on_video_cum
	, coalesce(activity_cum.total_active_on_video_gp_cum,0)
		as total_active_on_video_gp_cum

	-- Jinja loop to handle coalesces and family member types
	{% for family_member_type in ["Employee", "Dependent", "Child"] %}

	, usage.{{family_member_type}}_invited_count_cum
	, usage.{{family_member_type}}_invited_count
	, usage.{{family_member_type}}_signed_up_count_cum
	, usage.{{family_member_type}}_signed_up_count
	, usage.{{family_member_type}}_activated_count_cum
	, usage.{{family_member_type}}_activated_count

	, coalesce(activity.total_active_users_{{family_member_type}},0)
		as total_active_users_{{family_member_type}}

	, coalesce(activity.total_daus_{{family_member_type}},0)
		as total_daus_{{family_member_type}}
	, coalesce(activity.total_active_on_chat_{{family_member_type}},0)
		as total_active_on_chat_{{family_member_type}}
	, coalesce(activity.total_active_on_video_{{family_member_type}},0)
		as total_active_on_video_{{family_member_type}}
	, coalesce(activity.total_active_on_video_gp_{{family_member_type}},0)
		as total_active_on_video_gp_{{family_member_type}}

	, coalesce(activity_cum.total_daus_{{family_member_type}}_cum,0)
		as total_daus_{{family_member_type}}_cum
	, coalesce(activity_cum.total_active_on_chat_{{family_member_type}}_cum,0)
		as total_active_on_chat_{{family_member_type}}_cum
	, coalesce(activity_cum.total_active_on_video_{{family_member_type}}_cum,0)
		as total_active_on_video_{{family_member_type}}_cum
	, coalesce(activity_cum.total_active_on_video_gp_{{family_member_type}}_cum,0)
		as total_active_on_video_gp_{{family_member_type}}_cum

	{% endfor %}


from usage
left join activity
	using (date_month, organization_id)
left join activity_cum
	using (date_month, organization_id)
