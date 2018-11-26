with
	opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, closed_won as (
		select date_trunc('quarter', close_date) as date_quarter
			, date_trunc('week', close_date) as date_week
			, sum(amount) as closed_mrr
			, sum(amount) filter(where owner_province <> 'QC') as closed_mrr_roc
			, sum(amount) filter(
				where channel = 'Partner' and partner_influence = 'Full involvement'
			) as closed_mrr_partner_lead
			, sum(amount) filter(
				where channel = 'Partner' and partner_influence <> 'Full involvement'
			) as closed_mrr_partner_influenced
		from opportunities
		where is_won
		group by 1,2
	)

select date_quarter
	, date_week
	, closed_mrr as weekly_closed_mrr
	, closed_mrr_roc as weekly_closed_mrr_roc
	, closed_mrr_partner_lead as weekly_closed_mrr_partner_lead
	, closed_mrr_partner_influenced as weekly_closed_mrr_partner_influenced
	, sum(closed_mrr) over(
		partition by date_quarter
		order by date_week
	) as closed_mrr
	, sum(closed_mrr_roc) over(
		partition by date_quarter
		order by date_week
	) as closed_mrr_roc
	, sum(closed_mrr_partner_lead) over(
		partition by date_quarter
		order by date_week
	) as closed_mrr_partner_lead
	, sum(closed_mrr_partner_influenced) over(
		partition by date_quarter
		order by date_week
	) as closed_mrr_partner_influenced
from closed_won
group by 1,2,3,4,5,6
