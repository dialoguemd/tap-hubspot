select plan_id
	, feature_id
	, created::timestamp as created_at
from {{ ref('data_scribe_plan_features') }}
