with episode_details as (
		select * from {{ ref ( 'tableau_product_one_metric_resolution_rate' ) }}
	)

	, users as (
        select * from {{ ref ( 'scribe_users_detailed' ) }}
    )

    , user_contract as (
    	select * from {{ ref ( 'user_contract' ) }}
    )


select users.user_id
	, user_contract.contract_id
	, users.created_at as invited_at
	, users.signed_up_at
	, episode_details.episode_started_at as activated_at
	, episode_details.episode_resolved_at as first_resolve_at
	, case when fully_resolved then episode_details.episode_resolved_at
		else null end as first_full_resolve_at
	, extract('day' from users.signed_up_at
		- users.created_at) as days_since_invite_at_sign_up
	, extract('day' from episode_details.episode_resolved_at
		- users.signed_up_at) as days_signed_up_at_first_resolve
	, extract('day' from episode_details.episode_resolved_at
		- users.created_at) as days_since_invite_at_first_resolve
	, extract('day' from episode_details.episode_started_at
		- users.signed_up_at) as days_signed_up_at_activation
	, extract('day' from episode_details.episode_started_at
		- users.created_at) as days_since_invite_at_activation
	, extract('day' from current_date
		- users.signed_up_at) as days_signed_up
	, extract('day' from current_date
		- users.created_at) as days_since_invite
from user_contract
inner join users
	on user_contract.user_id = users.user_id
left join episode_details
	on user_contract.user_id = episode_details.patient_id
	and user_contract.during_est @> episode_details.episode_started_at
where (episode_details.rank = 1 or episode_details.rank is null)
	and lower(user_contract.family_member_type) = 'employee'
