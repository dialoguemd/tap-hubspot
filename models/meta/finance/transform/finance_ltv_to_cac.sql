with
	cac as (
		select * from {{ ref('xero_cac') }}
	)

	, ltv as (
		select * from {{ ref('salesforce_ltv')}}
	)

	select cac.date_month
		, cac.cost_total
		, cac.cost_eae
		, cac.cost_ae
		, ltv.mrr_signed
		, ltv.mrr_signed_eae
		, ltv.mrr_signed_ae
	from cac
	left join ltv
		using (date_month)
