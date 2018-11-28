select id as event_id
	, timestamp
	, context_app_name
	, context_app_bundle_id
	, context_app_build
	, context_app_version
	, context_device_id
	, context_device_manufacturer
	, context_device_model
	, context_device_name
	, context_locale
	, context_location_country
	, context_os_name
	, case
		when context_app_name = 'Dialogue Patient Desktop' then 'Desktop'
		when context_os_name = 'iPhone OS' then 'iOS'
		when context_os_name is not null then context_os_name
		else 'Desktop'
	end as platform_name
	, context_os_version
	, context_screen_height
	, context_screen_width
	, channel_id
	, post_id
	, user_id
	, channel_id as episode_id
	, context_user_agent
from patientapp.submit_post_success
{% if target.name == 'dev' %}
	where timestamp > current_timestamp - interval '1 months'
{% endif %}

-- Query to extract platform and browser name from the user agent
--
-- with
-- 	event1 as (
-- 		select id as event_id
-- 			, timestamp
-- 			, context_app_name
-- 			, context_app_bundle_id
-- 			, context_app_build
-- 			, context_app_version
-- 			, context_device_id
-- 			, context_device_manufacturer
-- 			, context_device_model
-- 			, context_device_name
-- 			, context_locale
-- 			, context_location_country
-- 			, context_os_name
-- 			, context_os_version
-- 			, context_screen_height
-- 			, context_screen_width
-- 			, channel_id
-- 			, post_id
-- 			, user_id
-- 			, context_user_agent
-- 			, case
-- 				when context_user_agent like '%Firefox/%' then 'Firefox'
-- 		        when context_user_agent like '%Chrome/%' or context_user_agent like '%CriOS%' then 'Chrome'
-- 				when context_user_agent like '%MSIE %' then 'IE'
-- 				when context_user_agent like '%MSIE+%' then 'IE'
-- 				when context_user_agent like '%Trident%' then 'IE'
-- 				when context_user_agent like '%iPhone%' then 'iPhone Safari'
-- 				when context_user_agent like '%iPad%' then 'iPad Safari'
-- 				when context_user_agent like '%Opera%' then 'Opera'
-- 				when context_user_agent like '%BlackBerry%' and context_user_agent like '%Version/%' then 'BlackBerry WebKit'       
-- 				when context_user_agent like '%BlackBerry%' then 'BlackBerry'
-- 				when context_user_agent like '%Android%' then 'Android'
-- 				when context_user_agent like '%Safari%' then 'Safari'
-- 				when context_user_agent like '%bot%' then 'Bot'
-- 				when context_user_agent like '%http://%' then 'Bot'
-- 				when context_user_agent like '%www.%' then 'Bot'
-- 				when context_user_agent like '%Wget%' then 'Bot'
-- 				when context_user_agent like '%curl%' then 'Bot'
-- 				when context_user_agent like '%urllib%' then 'Bot'
--         		else 'Unknown'
--     		end as browser_name
--     		, case
--     			when context_user_agent like '%Android%' then 'Android'
--     			when context_user_agent like '%Android%' then 'iOS'
--     		end as os_name
-- 			, POSITION('(' IN context_user_agent) + 1 as platform_start
-- 		from patientapp.submit_post_success
-- 	)

-- 	, event2 as (
-- 		select *
-- 			, SUBSTRING(context_user_agent, platform_start, 100) as platform_raw
-- 		from event1
-- 	)

-- 	, event3 as (
-- 		select *
-- 			, case when position(';' in platform_raw) = 0
-- 		        then POSITION(')' in platform_raw)
-- 		        else POSITION(';' in platform_raw)
-- 		    end as platform_end
-- 		from event2
-- 	)

-- 	, event4 as (
-- 		select *
-- 			, case when platform_end = 0 then 0 ELSE platform_end - 1 END platform_end2
-- 		from event3
-- 	)

-- select event_id
-- 	, timestamp
-- 	, context_app_name
-- 	, context_app_bundle_id
-- 	, context_app_build
-- 	, context_app_version
-- 	, context_device_id
-- 	, context_device_manufacturer
-- 	, context_device_model
-- 	, context_device_name
-- 	, context_locale
-- 	, context_location_country
-- 	, context_os_name
-- 	, context_os_version
-- 	, context_screen_height
-- 	, context_screen_width
-- 	, context_user_agent
-- 	, channel_id
-- 	, post_id
-- 	, user_id
-- 	, substring(context_user_agent, platform_start, platform_end2) as platform
-- 	, browser_name
-- from event4
