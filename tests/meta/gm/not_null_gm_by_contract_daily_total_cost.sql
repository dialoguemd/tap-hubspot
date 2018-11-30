with
	gm as (
		select * from {{ ref('gm_by_contract_daily') }}
	)

select *
from gm
where date_day > current_date - interval '1 week'
    and total_cost is null
