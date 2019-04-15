with
	nps_survey as (
		select * from {{ ref('nps_patient_survey') }}
	)

	, ranked_short as (
		select organization_id || ' - ' || organization_name as organization_name_id
			, language
			, nth_value(comment, 1)
				over (partition by organization_id, language order by timestamp desc)
				as short_comment_first_most_recent
			, nth_value(comment, 2)
				over (partition by organization_id, language order by timestamp desc)
				as short_comment_second_most_recent
			, nth_value(comment, 3)
				over (partition by organization_id, language order by timestamp desc)
				as short_comment_third_most_recent
		from nps_survey
		where comment_char_length between 50 and 150
			-- TODO: replace with tag regex for testimonials
			-- and lower(tags::text) like '%appreciation%'
	)

	, ranked_long as (
		select organization_id || ' - ' || organization_name as organization_name_id
			, language
			, nth_value(comment, 1)
				over (partition by organization_id, language order by timestamp desc)
				as long_comment_first_most_recent
			, nth_value(comment, 2)
				over (partition by organization_id, language order by timestamp desc)
				as long_comment_second_most_recent
			, nth_value(comment, 3)
				over (partition by organization_id, language order by timestamp desc)
				as long_comment_third_most_recent
		from nps_survey
		where comment_char_length between 150 and 500
			-- TODO: replace with tag regex for testimonials
			-- and lower(tags::text) like '%appreciation%'
	)

select organization_name_id
	, language
	, min(short_comment_first_most_recent) as short_comment_first_most_recent
	, min(short_comment_second_most_recent) as short_comment_second_most_recent
	, min(short_comment_third_most_recent) as short_comment_third_most_recent
	, min(long_comment_first_most_recent) as long_comment_first_most_recent
	, min(long_comment_second_most_recent) as long_comment_second_most_recent
	, min(long_comment_third_most_recent) as long_comment_third_most_recent
from ranked_short
left join ranked_long
	using (organization_name_id, language)
group by 1,2
