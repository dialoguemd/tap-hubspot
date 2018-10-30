with
	cac as (
		select *
		from {{ ref('xero_cac') }}
	),

	ltv as (
		select *
		from {{ ref('salesforce_ltv')}}
	)

	select cac.date_month
		, cac.cost_eae_qc
		, cac.cost_eae_roc
		, cac.cost_ae_qc
		, cac.cost_ae_roc
		, coalesce(ltv.mrr_signed, 0) as mrr_signed
		, coalesce(ltv.mrr_signed_eae_qc, 0) as mrr_signed_eae_qc
		, coalesce(ltv.mrr_signed_eae_roc, 0) as mrr_signed_eae_roc
		, coalesce(ltv.mrr_signed_ae_qc, 0) as mrr_signed_ae_qc
		, coalesce(ltv.mrr_signed_ae_roc, 0) as mrr_signed_ae_roc
	from cac
	left join ltv
		using (date_month)
