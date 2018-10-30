select *
	, eae_qc_perc + eae_roc_perc + ae_qc_perc + ae_roc_perc as total
from {{ ref('salesforce_eae_headcount_ratio_monthly') }}
where eae_qc_perc + eae_roc_perc + ae_qc_perc + ae_roc_perc < .999999
	or eae_qc_perc + eae_roc_perc + ae_qc_perc + ae_roc_perc > 1.000001
