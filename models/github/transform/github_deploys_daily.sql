with deploys as (
		select * from {{ ref( 'github_deploys' ) }}
	)

select date_trunc('day', merged_at) as merged_at_date
	, count(*) as deploys_count
from deploys
group by 1
