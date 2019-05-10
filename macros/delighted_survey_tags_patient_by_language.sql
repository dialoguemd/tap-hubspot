{% macro delighted_survey_tag_patient(lang) %}

select md5('{{lang}}' || _sdc_source_key_id) as survey_id
	, case
        when _sdc_value like '+ %' then trim(leading '+ ' from _sdc_value)
        when _sdc_value like '- %' then trim(leading '- ' from _sdc_value)
        else _sdc_value
        end as tag
    , case
        when _sdc_value like '+ %' then 'positive'
        when _sdc_value like '- %' then 'negative'
        else null
        end as sentiment
from tap_delighted_patient_{{lang}}.survey_responses__tags

{% endmacro %}
