with
	nps_survey_en as (
		select * from {{ ref('delighted_survey_patient_en') }}
	)

	, nps_survey_fr as (
		select * from {{ ref('delighted_survey_patient_fr') }}
	)

	, tags as (
		select * from {{ ref('delighted_survey_tags_patient') }}
	)

	, tags_aggregated as (
		select survey_id
			, bool_or(tag = 'testimonial') as is_testimonial
			, string_agg(distinct tag, ', ') as tags
		from tags
		group by 1
	)

	, unioned as (
		select * from nps_survey_en
		union all
		select * from nps_survey_fr
	)

select *
	, char_length(unioned.comment) as comment_char_length
from unioned
left join tags_aggregated
	using (survey_id)
