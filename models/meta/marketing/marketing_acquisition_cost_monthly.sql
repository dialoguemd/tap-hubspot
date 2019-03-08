with
	opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, funnelio as (
		select * from {{ ref('funnelio_performance_metrics') }}
	)

	, meetings_monthly as (
		select date_trunc('month', meeting_date) as date_month
			, count(*) as sql_count
			, sum(amount) as sql_mrr
			, sum(
				amount
				* case
					when segment = '1-39' then .42
					when segment = '40-99' then .32
					when segment = '100-199' then .15
					when segment = '200-499' then .13
					when segment = '500-999' then .12
					when segment = '1000+' then .07
				end
			) as sql_mrr_weighted
			, count(*) filter(where initiate_date is not null) as sqo_count
			, sum(amount) filter(where initiate_date is not null) as sqo_mrr
		from opportunities
		where is_inbound
		group by 1
	)

	, costs_monthly as (
		select date_month
		    , sum(cost) as cost
		from funnelio
		-- exclude Germany costs
		where channel not like '%_DE_%'
		group by 1
	)

select costs_monthly.date_month
	, costs_monthly.cost
	, coalesce(meetings_monthly.sql_count, 0) as sql_count
	, coalesce(meetings_monthly.sql_mrr, 0) as sql_mrr
	, coalesce(meetings_monthly.sql_mrr_weighted, 0) as sql_mrr_weighted
	, coalesce(meetings_monthly.sqo_count, 0) as sqo_count
	, coalesce(meetings_monthly.sqo_mrr, 0) as sqo_mrr
from costs_monthly
left join meetings_monthly
	using (date_month)
