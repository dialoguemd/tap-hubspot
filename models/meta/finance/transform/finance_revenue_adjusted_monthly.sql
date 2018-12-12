with adj as (
	    select * from {{ ref('finance_revenue_adjustment') }}
	)

	, orgs as (
	    select * from {{ ref('organizations') }}
	)

select adj.month
    , adj.recognized
    , adj.mh
    , adj.account
    , orgs.organization_name
    , orgs.organization_id
from adj
left join orgs
    on (lower(adj.account) like '%' || lower(orgs.organization_name) || '%'
    	or adj.org_name = orgs.organization_name)
    and organization_id not in (18)
