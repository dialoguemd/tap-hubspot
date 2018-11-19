select date_month
	, cost_eae + cost_ae as total_summed
	, cost_total
	, cost_eae
	, cost_ae
from {{ ref('xero_cac') }}
where (cost_eae + cost_ae)::int
	<> cost_total::int
