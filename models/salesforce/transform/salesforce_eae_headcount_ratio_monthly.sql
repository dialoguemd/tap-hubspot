with

	monthly_users_by_title as (
		select * from {{ ref('salesforce_monthly_users_by_title') }}
	)

select date_month
	, coalesce(
		sum(user_count) filter(where
			title = 'Enterprise Account Executive'
			and province = 'QC'
			) / sum(user_count)
		, 0) as eae_qc_perc
	, coalesce(
		sum(user_count) filter(where
			title = 'Enterprise Account Executive'
			and province <> 'QC'
			) / sum(user_count)
		, 0) as eae_roc_perc
	, coalesce(
		sum(user_count) filter(where
			title = 'Account Executive'
			and province = 'QC'
			) / sum(user_count)
		, 0) as ae_qc_perc
	, coalesce(
		sum(user_count) filter(where
			title = 'Account Executive'
			and province <> 'QC'
			) / sum(user_count)
		, 0) as ae_roc_perc
from monthly_users_by_title
where title in ('Account Executive', 'Enterprise Account Executive')
group by 1
