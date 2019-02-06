with
	cs_organization_monthly as (
		select * from {{ ref('cs_organization_monthly') }}
	)

select *
from cs_organization_monthly
where date_month < '2016-01-01'
