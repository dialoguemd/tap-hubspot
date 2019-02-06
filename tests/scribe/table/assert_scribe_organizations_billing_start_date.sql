with
	scribe_organizations_detailed as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

select *
from scribe_organizations_detailed
where billing_start_date is null
	or billing_start_date < '2016-01-01'
