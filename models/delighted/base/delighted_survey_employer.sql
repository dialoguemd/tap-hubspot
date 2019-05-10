select to_timestamp(created_at) as created_at
	, to_timestamp(created_at) as timestamp
    , to_timestamp(updated_at) as updated_at

	{% for timeframe in ['day', 'week', 'month'] %}

	, date_trunc('{{timeframe}}', to_timestamp(created_at))
		as date_{{timeframe}}

	{% endfor %}

    , id as survey_id
    , score
    , case
        when score < 7 then 'detractor'
        when score < 9 then 'passive'
        when score >=9 then 'promoter'
        else null
        end as category
    , comment
from tap_delighted_employer.survey_responses
