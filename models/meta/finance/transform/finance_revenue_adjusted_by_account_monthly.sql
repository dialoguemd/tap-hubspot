with journal_entries as (
	    select * from {{ ref('finance_revenue_adjusted_monthly') }}
	)

select month
    , account_name
    , min(billing_start_date) as billing_start_date
    , sum(recognized) + sum(mh) as recognized
    , row_number() over (partition by account_name order by month) as rank
from journal_entries
group by 1,2
