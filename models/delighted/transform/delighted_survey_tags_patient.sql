with
	tags_en as (
		select * from {{ ref('delighted_survey_tags_patient_en') }}
	)

	, tags_fr as (
		select * from {{ ref('delighted_survey_tags_patient_fr') }}
	)

select * from tags_en
union all
select * from tags_fr
