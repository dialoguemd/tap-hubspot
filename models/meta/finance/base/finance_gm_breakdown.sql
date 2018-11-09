select date_month
	, gm_source
	, value::float as value
from {{ ref('data_finance_gm_breakdown') }}
