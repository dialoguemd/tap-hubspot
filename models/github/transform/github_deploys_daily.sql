with
	deploys as (
		select * from {{ ref('github_deploys') }}
	)

select date_trunc('day', merged_at) as merged_at_date
	, count(*) filter (where type_development = 'Production')
		as prod_dev_deploys_count
	, count(*) filter (where type_development in ('Production', 'Non-Prod'))
		as dev_deploys_count
	, count(*) filter (where type_development = 'Data-Infrastructure')
		as data_deploys_count
	, count(*) as deploys_count
from deploys
group by 1
