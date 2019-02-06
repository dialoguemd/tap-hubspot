with
	scribe_organizations_detailed as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

select *
from scribe_organizations_detailed
where (charge_price = 0 and charge_strategy <> 'free')
	or (charge_price <> 0 and charge_strategy = 'free')
