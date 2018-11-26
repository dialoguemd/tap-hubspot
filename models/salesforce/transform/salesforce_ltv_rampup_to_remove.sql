with
	months as (
		select date_month
		from {{ ref('dimension_months') }}
		where date_month >= '2017-01-01'
			and date_month <= date_trunc('month', current_date)
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, employment_start_dates as (
		select * from {{ ref('salesforce_employment_start_dates') }}
	)

	, monthly_signed as (
		select date_trunc('month', opportunities.close_date) as date_month
			, coalesce(
				- sum(opportunities.amount)
					filter (where opportunities.segment <> '1000+')
				, 0) as mrr_signed_ae
			, coalesce(
				- sum(opportunities.amount)
					filter (where opportunities.segment = '1000+')
				, 0) as mrr_signed_eae
		from opportunities
		inner join employment_start_dates
			on opportunities.owner_id = employment_start_dates.user_id
		where opportunities.close_date
			< employment_start_dates.start_date + interval '6 months'
			and opportunities.is_won
		group by 1
	)

select months.date_month
	, coalesce(mrr_signed_ae, 0) as mrr_signed_ae
	, coalesce(mrr_signed_eae, 0) as mrr_signed_eae
from months
left join monthly_signed
	using (date_month)
