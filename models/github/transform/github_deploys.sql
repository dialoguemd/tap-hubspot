with
	prs as (
		select * from {{ ref('github_pull_requests') }}
	)

	, whitelist as (
		select * from {{ ref('github_repo_whitelist') }}
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
	, case
		when whitelist.stage = 'Production'
			and whitelist.type = 'Code'
			and prs.merged_at > whitelist.whitelist_date
			then 'Production'
		when whitelist.stage = 'Development'
			and whitelist.type = 'Code'
			and prs.merged_at > whitelist.whitelist_date
			then 'Non-Prod'
		when whitelist.stage = 'Data-Infrastructure'
			then 'Data-Infrastructure'
		else 'Other'
		end as type_development
from prs
left join whitelist using (repo_name)
where (prs.base_branch = 'master'
		or (prs.base_branch = 'beta' and prs.repo_name = 'care-platform'))
	and prs.merged_at is not null
