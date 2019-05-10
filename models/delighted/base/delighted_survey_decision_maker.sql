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
    , person__id as delighted_user_id
    , person__email as email
    , person_properties__contact_type as contact_type
    , person_properties__month_since_billing_start_date::bigint
        as month_since_billing_start_date
    , person_properties__organization_id::integer as organization_id
from tap_delighted_decision_maker.survey_responses
