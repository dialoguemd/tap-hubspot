with
	cac as (
		select * from {{ ref('xero_cac') }}
	)

	, ltv as (
		select * from {{ ref('salesforce_ltv')}}
	)

	select cac.date_month
		, cac.cost_eae_qc
		, cac.cost_eae_roc
		, cac.cost_ae_qc
		, cac.cost_ae_roc
		, ltv.mrr_signed
		, ltv.mrr_signed_eae_qc
		, ltv.mrr_signed_eae_roc
		, ltv.mrr_signed_ae_qc
		, ltv.mrr_signed_ae_roc
	from cac
	left join ltv
		using (date_month)
