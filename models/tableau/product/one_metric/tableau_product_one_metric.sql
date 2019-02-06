with episodes as (
		select * from {{ ref('episodes_with_contracts') }}
	)

	, chats as (
		select * from {{ ref('chats') }}
	)

	, user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

	, eps_ranked as (
		select user_id
			, episode_id
			, attr_total
			, score as nps_score
			, issue_type
			, outcome
			, includes_video_np
			, includes_video_gp
			, includes_video_nc
			, includes_video_cc
			, includes_video_psy
			, includes_video
			, timezone('America/Montreal', first_message_patient) as episode_started
			, date_trunc('day',
				timezone('America/Montreal', first_set_resolved_pending_at)
				) as resolved_at_day
			, row_number() over(partition by user_id order by first_set_resolved_pending_at asc) as rank
		from episodes
		where outcome
				not in ('inappropriate_profile'
						, 'follow_up'
						, 'patient_thanks'
						, 'episode_duplicate'
						, 'new_dependant'
						, 'test'
						, 'audit'
						, 'admin'
						, 'closed_after_follow_up'
						, 'patient_unresponsive')
			and issue_type
				not in ('test'
						, 'admin')
			and first_set_resolved_pending_at is not null
	)
	
	, first_ep_details as (
		select eps_ranked.user_id
			, eps_ranked.episode_id
			, eps_ranked.attr_total
			, eps_ranked.nps_score
			, eps_ranked.issue_type
			, eps_ranked.outcome
			, eps_ranked.includes_video_np
			, eps_ranked.includes_video_gp
			, eps_ranked.includes_video_nc
			, eps_ranked.includes_video_cc
			, eps_ranked.includes_video_psy
			, eps_ranked.includes_video
			, eps_ranked.episode_started
			, eps_ranked.resolved_at_day
			, min(chats.wait_time_first_care_team) as wait_time_first
			, min(chats.wait_time_first_nurse) as wait_time_first_nurse
		from eps_ranked
		left join chats
			on eps_ranked.user_id = chats.user_id
			and eps_ranked.episode_id = chats.episode_id
			and eps_ranked.resolved_at_day = chats.date_day_est
		where eps_ranked.rank = 1
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
	)
   
	, retention_stats as (
		select first_ep_details.user_id
			, count(distinct episodes.episode_id) as count_episodes
			, extract(epoch from current_date - 
				min(first_ep_details.resolved_at_day)) / 86400 as age_in_days
		from first_ep_details
		left join episodes
			on first_ep_details.user_id = episodes.user_id
			and tstzrange(
					first_ep_details.episode_started,
					first_ep_details.episode_started + interval '120 days'
				) @>
				timezone('America/Montreal',
					episodes.first_message_patient)
			and episodes.first_set_resolved_pending_at is not null
		group by 1
	) 
	
select first_ep_details.*
	, retention_stats.count_episodes
	-- contract_id for testing uniqueness
	, user_contract.contract_id
	, user_contract.organization_name
from retention_stats
left join first_ep_details using (user_id)
left join user_contract
	on retention_stats.user_id = user_contract.user_id
	and first_ep_details.resolved_at_day <@ user_contract.during
where age_in_days > 120
	and count_episodes > 0
