select date_month
	, forecast::float as forecast
from {{ ref('data_finance_runway') }}
