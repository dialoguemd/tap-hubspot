with
	answers_en as (
		select * from {{ ref('delighted_survey_additional_answers_patient_en') }}
	)

	, answers_fr as (
		select * from {{ ref('delighted_survey_additional_answers_patient_fr') }}
	)

select * from answers_en
union all
select * from answers_fr
