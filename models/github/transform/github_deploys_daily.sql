with prs as (
        select * from {{ ref( 'github_pull_requests' ) }}
    )

    , nonprod_repos as (
        select * from {{ ref( 'github_nonprod_repos' ) }}
    )

select date_trunc('day', merged_at) as merged_at_date
    , coalesce(count(*),0) as deploys_count
from prs
left join nonprod_repos using (repo_name)
where (base_branch = 'master'
        or (base_branch = 'beta' and repo_name = 'care-platform'))
   and merged_at is not null
   and date_trunc('week', current_date) <> date_trunc('week', merged_at)
   and nonprod_repos.repo_name is null
group by 1
