select id as feature_id
	, label as feature
	, localized_name
	, localized_description
	, created as created_at
from {{ ref('data_scribe_features') }}
