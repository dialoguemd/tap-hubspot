with prs as (
		select * from {{ ref( 'github_pull_requests' ) }}
	)

	, nonprod_repos as (
		select * from {{ ref( 'github_nonprod_repos' ) }}
	)

select prs.number 
	, prs.title
	, prs.state
	, prs.html_url
	, prs.created_at
	, prs.merged_at
	, prs.base_branch
	, coalesce(prs.repo_name,
		split_part(
			replace(prs.html_url, 'https://github.com/dialoguemd/', ''),
			'/',1),
		null) as repo_name
from prs
left join nonprod_repos using (repo_name)
where (prs.base_branch = 'master'
		or (prs.base_branch = 'beta' and prs.repo_name = 'care-platform'))
	and prs.merged_at is not null
	and date_trunc('week', current_date) <> date_trunc('week', prs.merged_at)
	and nonprod_repos.repo_name is null
