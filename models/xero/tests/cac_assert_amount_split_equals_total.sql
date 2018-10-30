select date_month
	, cost_eae_qc + cost_eae_roc + cost_ae_qc + cost_ae_roc as total_summed
	, cost_total
	, cost_eae_qc
	, cost_eae_roc
	, cost_ae_qc
	, cost_ae_roc
from {{ ref('xero_cac') }}
where (cost_eae_qc + cost_eae_roc + cost_ae_qc + cost_ae_roc)::int
	<> cost_total::int
