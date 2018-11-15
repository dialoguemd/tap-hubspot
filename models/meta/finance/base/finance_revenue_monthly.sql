select date_month
	, telehealth_revenue::float as telehealth_revenue
	, mrr::float as mrr
from {{ ref('data_finance_revenue_monthly') }}
