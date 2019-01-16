with episodes as (
      select * from {{ ref( 'episodes' ) }}
   )

   , issue_type_labels as (
      select * from {{ ref( 'medops_issue_type_labels' ) }}
   )

select date_trunc('month', episodes.first_message_patient) as date_month
	, episodes.organization_name
	, episodes.account_name
	, episodes.residence_province
	, issue_type_labels.label_en as issue_type_en
	, issue_type_labels.label_fr as issue_type_fr
	, count(episodes.episode_id) as count_episodes
from episodes
inner join issue_type_labels
	using (issue_type)
where episodes.first_message_patient is not null
group by 1,2,3,4,5,6
