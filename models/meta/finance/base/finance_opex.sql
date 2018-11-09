select date_month
	, department
	, delta::float as delta
from {{ ref('data_finance_opex') }}
