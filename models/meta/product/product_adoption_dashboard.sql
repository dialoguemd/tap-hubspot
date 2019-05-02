{{
  config(
    enabled=false
  )
}}

with
	posts_all_time as (
		select * from {{ ref('messaging_posts_all_time')}}
	)

	, file_uploaded as (
		select * from {{ ref('careplatform_file_uploaded')}}
	)

	, message_retracted as (
		select * from {{ ref('careplatform_message_retracted')}}
	)

	, video_started as (
		select * from {{ ref('careplatform_video_stream_created')}}
	)

	, slash_command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered')}}
	)

	, update_episode_properties as (
		select * from {{ ref('careplatform_episode_properties_updated')}}
	)

	, update_episode_state as (
		select * from {{ ref('usher_episode_state_updated')}}
	)

	, episode_assigned as (
		select * from {{ ref('usher_episode_assigned')}}
	)

	, reminder_created as (
		select * from {{ ref('careplatform_reminder_created')}}
	)

	, note_created as (
		select * from {{ ref('careplatform_note_created')}}
	)

	, electron_reload as (
		select * from {{ ref('careplatform_electron_reload')}}
	)

	, doctor_add as (
		select * from {{ ref('careplatform_family_doctor_added')}}
	)

	, doctor_updated as (
		select * from {{ ref('careplatform_family_doctor_updated')}}
	)

	, siderbar_clicked as (
		select * from {{ ref('careplatform_sidebar_clicked')}}
	)

	, snooze_clicked as (
		select * from {{ ref('careplatform_snooze_started')}}
	)

	, wiw_shifts as (
		select * from {{ ref('wiw_shifts_detailed')}}
	)

	, practitioners as (
		select * from {{ ref('practitioners')}}
	)

	, chats as (
		select date_trunc('day', created_at) as date
			, episode_id
			, user_id
			, 'chat' as feature
			, 'chat' as feature_category
			, count(*)
		from posts_all_time
		where is_internal_post is false
		group by 1,2,3,4,5
	)

	, internal_chats as (
		select date_trunc('day', created_at) as date
			, episode_id
			, user_id
			, 'internal_chat' as feature
			, 'chat' as feature_category
			, count(*)
		from posts_all_time
		where post_type = 'dialogue_system'
		and user_id is not null
		group by 1,2,3,4,5
	)

	, file_uploads as (
		select date_trunc('day', timestamp) as date
			, coalesce(episode_id, 'no_episode_id') as episode_id
			, user_id
			, 'file_upload' as feature
			, 'chat' as feature_category
			, count(*)
		from file_uploaded
		group by 1,2,3,4,5
	)

	, mentions as (
		select date_trunc('day', created_at) as date
		, episode_id
		, user_id
		, 'mention' as feature
		, 'chat' as feature_category
		, count(*)
		from posts_all_time
		where mention is not null
		group by 1,2,3,4,5
	)

	, retract_messages as (
		select date_trunc('day', timestamp) as date
		, episode_id
		, user_id
		, 'retract_message' as feature
		, 'chat' as feature_category
		, count(*)
		from message_retracted
		group by 1,2,3,4,5
	)

	, videos as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, practitioner_id as user_id
			, 'video' as feature
			, 'chat' as feature_category
			, count(*)
		from video_started
		group by 1,2,3,4,5
	)

	, templates as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, coalesce(command_name, 'unknown_command') as feature
			, 'slash_command' as feature_category
			, count(*)
		from slash_command_triggered
		group by 1,2,3,4,5
	)

	, episode_properties as (
		select date_trunc('day', timestamp) as date
			, coalesce(episode_id, 'no_episode_id') as episode_id
			, user_id
			, episode_property_type as feature
			, 'episode_properties' as feature_category
			, count(*)
		from update_episode_properties
		where episode_property_type is not null
		group by 1,2,3,4,5
	)

	, episode_state as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'set_to_' || episode_state as feature
			, 'episode_state' as feature_category
			, count(*)
		from update_episode_state
		where episode_state != 'active'
		group by 1,2,3,4,5
	)

	, assignments as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'assignment' as feature
			, 'right_pane' as feature_category
			, count(*)
		from episode_assigned
		where assigned_user_id is not null
		group by 1,2,3,4,5
	)

	, reminders as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'reminder' as feature
			, 'right_pane' as feature_category
			, count(*)
		from reminder_created
		group by 1,2,3,4,5
	)

	, notes as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'notes' as feature
			, 'right_pane' as feature_category
			, count(*)
		from note_created
		group by 1,2,3,4,5
	)

	, force_reloads as (
		select date_trunc('day', timestamp) as date
			, coalesce(episode_id, 'no_episode_id') as episode_id
			, user_id
			, 'force_reloads' as feature
			, 'other' as feature_category
			, count(*)
		from electron_reload
		where forced
		group by 1,2,3,4,5
	)

	, reloads as (
		select date_trunc('day', timestamp) as date
			, coalesce(episode_id, 'no_episode_id') as episode_id
			, user_id
			, 'reloads' as feature
			, 'other' as feature_category
			, count(*)
		from electron_reload
		where forced is false
		group by 1,2,3,4,5
	)

	, add_family_doctor as (
		select date_trunc('day', timestamp) as date
			, 'no_episode_id' as episode_id
			, user_id
			, 'add_family_doctor' as feature
			, 'left_pane' as feature_category
			, count(*)
		from doctor_add
		where family_doctor_last_name is not null
		and family_doctor_first_name is not null
		group by 1,2,3,4,5
	)

	, update_family_doctor as (
		select date_trunc('day', timestamp) as date
			, 'no_episode_id' as episode_id
			, user_id
			, 'update_family_doctor' as feature
			, 'left_pane' as feature_category
			, count(*)
		from doctor_updated
		where family_doctor_last_name is not null
		and family_doctor_first_name is not null
		group by 1,2,3,4,5
	)

	, sidebar_nav as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'sidebar_navigation' as feature
			, 'navigation' as feature_category
			, count(*)
		from siderbar_clicked
		group by 1,2,3,4,5
	)

	, snooze as (
		select date_trunc('day', timestamp) as date
			, episode_id
			, user_id
			, 'snooze' as feature
			, 'state_change' as feature_category
			, count(*)
		from snooze_clicked
		group by 1,2,3,4,5
	)

	, on_shift as (
		select start_day_est as date
		, 'no_episode_id' as episode_id
		, user_id
		, 'on_shift' as feature
		, 'on_shift' as feature_category
		, count(*)
		from wiw_shifts
		where location_name = 'Virtual Care Platform'
		and start_day_est < current_date
		group by 1,2,3,4,5
	)

	, unioned as (

		{% for table in
			['chats',
			'internal_chats',
			'file_uploads',
			'mentions',
			'retract_messages',
			'videos',
			'templates',
			'episode_properties',
			'episode_state',
			'assignments',
			'reminders',
			'notes',
			'force_reloads',
			'reloads',
			'add_family_doctor',
			'update_family_doctor',
			'sidebar_nav',
			'snooze']
		%}

		select * from {{table}}
		union all

		{% endfor %}

		select * from on_shift
	)

select md5(unioned.date::text ||
	unioned.user_id ||
	unioned.episode_id ||
	unioned.feature) as id
, unioned.date
, unioned.user_id
, practitioners.main_specialization
, unioned.episode_id
, unioned.feature
, unioned.feature_category
, unioned.count
from unioned
inner join practitioners on unioned.user_id = practitioners.user_id
inner join on_shift on unioned.user_id = on_shift.user_id
	and unioned.date = on_shift.date
	and unioned.date > '2018-01-01'
