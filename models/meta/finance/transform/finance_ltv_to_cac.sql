with
	cac as (
		select * from {{ ref('xero_cac') }}
	)

	, ltv as (
		select * from {{ ref('salesforce_ltv')}}
	)

	, ltv_ramp as (
		select * from {{ ref('salesforce_ltv_rampup_to_remove')}}
	)

	, cac_ramp as (
		select * from {{ ref('finance_rampup_cost') }}
	)

	, joined as (
		select cac.date_month
			, cac.cost_total
			, cac.cost_ae
			, cac.cost_eae

			, cac_ramp.cost_ae_ramp + cac_ramp.cost_eae_ramp
				as cost_total_ramp
			, cac_ramp.cost_ae_ramp
			, cac_ramp.cost_eae_ramp

			, cac.cost_total
				- (cac_ramp.cost_ae_ramp + cac_ramp.cost_eae_ramp)
				as cost_total_excl_ramp
			, cac.cost_ae - cac_ramp.cost_ae_ramp as cost_ae_excl_ramp
			, cac.cost_eae - cac_ramp.cost_eae_ramp as cost_eae_excl_ramp

			, ltv.mrr_signed
			, ltv.mrr_signed_ae
			, ltv.mrr_signed_eae

			, ltv_ramp.mrr_signed_ae + ltv_ramp.mrr_signed_eae
				as mrr_signed_ramp
			, ltv_ramp.mrr_signed_ae as mrr_signed_ae_ramp
			, ltv_ramp.mrr_signed_eae as mrr_signed_eae_ramp

			, ltv.mrr_signed
				- (ltv_ramp.mrr_signed_ae + ltv_ramp.mrr_signed_eae)
				as mrr_signed_excl_ramp
			, ltv.mrr_signed_ae - ltv_ramp.mrr_signed_ae as mrr_signed_ae_excl_ramp
			, ltv.mrr_signed_eae - ltv_ramp.mrr_signed_eae as mrr_signed_eae_excl_ramp
		from cac
		left join cac_ramp
			using (date_month)
		left join ltv
			using (date_month)
		left join ltv_ramp
			using (date_month)
	)


	, ltv_to_cac as (
		select date_month
			, cost_total
			, cost_eae
			, cost_ae
			, sum(cost_total) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_total_12_months
			, sum(cost_eae) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_eae_12_months
			, sum(cost_ae) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_ae_12_months

			, cost_total_excl_ramp
			, cost_eae_excl_ramp
			, cost_ae_excl_ramp
			, sum(cost_total_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_total_excl_ramp_12_months
			, sum(cost_eae_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_eae_excl_ramp_12_months
			, sum(cost_ae_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as cost_ae_excl_ramp_12_months

			, mrr_signed
			, mrr_signed_eae
			, mrr_signed_ae
			, sum(mrr_signed) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_12_months
			, sum(mrr_signed_eae) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_eae_12_months
			, sum(mrr_signed_ae) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_ae_12_months

			, mrr_signed_excl_ramp
			, mrr_signed_eae_excl_ramp
			, mrr_signed_ae_excl_ramp
			, sum(mrr_signed_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_excl_ramp_12_months
			, sum(mrr_signed_eae_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_eae_excl_ramp_12_months
			, sum(mrr_signed_ae_excl_ramp) over(
				order by date_month
				rows between 11 preceding and current row
			) as mrr_signed_ae_excl_ramp_12_months
		from joined
	)

select *

	, case
		when cost_total_12_months <> 0
		then mrr_signed_12_months / cost_total_12_months
			* .55 * 48
		else null
	end as ltv_to_cac
	, case
		when cost_ae_12_months <> 0
		then mrr_signed_ae_12_months / cost_ae_12_months
			* .55 * 48
		else null
	end as ltv_to_cac_ae
	, case
		when cost_eae_12_months <> 0
		then mrr_signed_eae_12_months / cost_eae_12_months
			* .55 * 48
		else null
	end as ltv_to_cac_eae
	, case
		when mrr_signed_12_months <> 0
		then cost_total_12_months / (mrr_signed_12_months * .55)
		else null
	end as payback_period
	, case
		when mrr_signed_ae_12_months <> 0
		then cost_ae_12_months / (mrr_signed_ae_12_months * .55)
		else null
	end as payback_period_ae
	, case
		when mrr_signed_eae_12_months <> 0
		then cost_eae_12_months / (mrr_signed_eae_12_months * .55)
		else null
	end as payback_period_eae


	, case
		when cost_total_excl_ramp_12_months <> 0
		then mrr_signed_excl_ramp_12_months
			/ cost_total_excl_ramp_12_months
			* .55 * 48
		else null
	end as ltv_to_cac_excl_ramp
	, case
		when cost_ae_excl_ramp_12_months <> 0
		then mrr_signed_ae_excl_ramp_12_months
			/ cost_ae_excl_ramp_12_months
			* .55 * 48
		else null
	end as ltv_to_cac_ae_excl_ramp
	, case
		when cost_eae_excl_ramp_12_months <> 0
		then mrr_signed_eae_excl_ramp_12_months
			/ cost_eae_excl_ramp_12_months
			* .55 * 48
		else null
	end as ltv_to_cac_eae_excl_ramp
	, case
		when mrr_signed_excl_ramp_12_months <> 0
		then cost_total_excl_ramp_12_months
			/ (mrr_signed_excl_ramp_12_months * .55)
		else null
	end as payback_period_excl_ramp
	, case
		when mrr_signed_ae_excl_ramp_12_months <> 0
		then cost_ae_excl_ramp_12_months
			/ (mrr_signed_ae_excl_ramp_12_months * .55)
		else null
	end as payback_period_ae_excl_ramp
	, case
		when mrr_signed_eae_excl_ramp_12_months <> 0
		then cost_eae_excl_ramp_12_months
			/ (mrr_signed_eae_excl_ramp_12_months * .55)
		else null
	end as payback_period_eae_excl_ramp
from ltv_to_cac
