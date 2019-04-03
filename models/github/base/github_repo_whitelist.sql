with
	repos as (
        select * from {{ ref('data_engineering_repo_white_list') }}
    )

select repo_name
	, stage
	, type
	, whitelist_date::date
from repos
