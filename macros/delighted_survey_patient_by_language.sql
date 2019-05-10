{% macro delighted_survey_patient(lang) %}

select to_timestamp(created_at) as created_at
	, to_timestamp(created_at) as timestamp
    , to_timestamp(updated_at) as updated_at

	{% for timeframe in ['day', 'week', 'month'] %}

	, date_trunc('{{timeframe}}', to_timestamp(created_at))
		as date_{{timeframe}}

	{% endfor %}

    , md5('{{lang}}' || id) as survey_id
    , score
    , case
        when score < 7 then 'detractor'
        when score < 9 then 'passive'
        when score >=9 then 'promoter'
        else null
        end as category
    , comment
    , person__id as delighted_user_id
    , person__email as email

    {% for field in ['episode_id', 'user_id', 'locale', 'organization_id', 'sex'] %}
    , person_properties__{{field}} as {{field}}
    {% endfor %}

    , '{{lang}}'::text as language
from tap_delighted_patient_{{lang}}.survey_responses

{% endmacro %}
