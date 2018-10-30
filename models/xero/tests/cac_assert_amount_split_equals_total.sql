select *
from {{ ref('xero_cac') }}
where cost_eae_qc
	+ cost_eae_roc
	+ cost_ae_qc
	+ cost_ae_roc
	<> cost_total