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
    , orgs.account_name
    , orgs.organization_id
    , orgs.billing_start_date
from adj
left join orgs
    on (lower(adj.account) like '%' || lower(orgs.organization_name) || '%'
    	or adj.org_name = orgs.organization_name)
    -- Exclude certain organizations due to the overlapping of their names
    and organization_id not in (18, 383, 575, 621, 619, 579)
