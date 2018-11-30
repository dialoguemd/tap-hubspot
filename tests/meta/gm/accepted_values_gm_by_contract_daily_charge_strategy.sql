with
	gm as (
		select * from {{ ref('gm_by_contract_daily') }}
	)

select *
from gm
where date_day > current_date - interval '1 week'
    and charge_strategy not in ('free', 'dynamic', 'auto_dynamic', 'fixed')
