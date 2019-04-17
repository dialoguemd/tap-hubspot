with episodes as (
        select * from {{ ref('episodes') }}
    )

	, user_contract as (
        select * from {{ ref('user_contract') }}
    )

	, valid_pairs as (
        select * from {{ ref('episodes_valid_outcome_issue_type_pairs') }}
    )

select episodes.episode_id
	, episodes.patient_id
	, episodes.outcome
	, case 
		when episodes.outcome = 'care_plan' and episodes.includes_video_consultation_gp
			then (episodes.outcome || '_gp')
		when episodes.outcome = 'care_plan' and episodes.includes_video_consultation_np
			then (episodes.outcome || '_np')
		else episodes.outcome
		end as outcome_alt
	, episodes.issue_type
	, episodes.issue_type_outcome_pair
	, case 
		when episodes.outcome = 'care_plan' and episodes.includes_video_consultation_gp
			then (episodes.issue_type_outcome_pair || '_gp')
		when episodes.outcome = 'care_plan' and episodes.includes_video_consultation_np
			then (episodes.issue_type_outcome_pair || '_np')
		else episodes.issue_type_outcome_pair
		end as issue_type_outcome_pairs_alt
	, episodes.priority_level
	, episodes.rating
	, episodes.score as nps_score
	, episodes.category as nps_category
	, user_contract.organization_name
	, episodes.first_message_patient as episode_started_at
	, episodes.first_set_resolved_pending_at as episode_resolved_at
	-- contract_id is for testing for uniqueness
	, user_contract.contract_id
	, user_contract.gender
	, user_contract.language
	, user_contract.is_signed_up
	, user_contract.signed_up_at
	, extract('day' from episodes.first_set_resolved_pending_at
		- user_contract.signed_up_at) as days_signed_up_at_first_resolve
	, extract('day' from current_date
		- user_contract.signed_up_at) as days_signed_up
	, row_number() over (partition by episodes.user_id
		order by episodes.first_set_resolved_pending_at desc) as rank
	, valid_pairs.valid_pairs is not null as fully_resolved
from episodes
inner join user_contract
	on episodes.patient_id = user_contract.user_id
	and user_contract.during_est @> episodes.first_message_patient
left join valid_pairs
	on episodes.issue_type_outcome_pair
		= valid_pairs.valid_pairs
