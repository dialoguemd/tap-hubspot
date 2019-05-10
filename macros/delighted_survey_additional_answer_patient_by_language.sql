{% macro delighted_survey_additional_answer_patient(lang) %}

select md5('{{lang}}' || _sdc_source_key_id) as survey_id
	-- TODO fix tap-delighted to print this as a workable json object
	, value
	, question
from tap_delighted_patient_{{lang}}.survey_responses__additional_answers

{% endmacro %}
