with episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

    , chats as (
        select * from {{ ref( 'chats_all_time' ) }}
    )

    , users as (
        select * from pdt.users
    )

    , eps_ranked as (
        select user_id
            , episode_id
            , attr_total
            , score as nps_score
            , issue_type
            , outcome
            , includes_np_video
            , includes_gp_video
            , includes_nc_video
            , includes_cc_video
            , includes_psy_video
            , includes_video
            , date_trunc('day', first_set_resolved_pending_at)
                as resolved_at_day
            , row_number() over(partition by user_id order by first_set_resolved_pending_at asc) as rank
        from episodes
        where outcome not in ('inappropriate_profile'
                            , 'follow_up'
                            , 'patient_thanks'
                            , 'episode_duplicate'
                            , 'new_dependant'
                            , 'test'
                            , 'audit'
                            , 'admin'
                            , 'closed_after_follow_up')
    )
    
    , first_ep_details as (
        select eps_ranked.*
            , chats.wait_time_first
            , chats.wait_time_first_nurse
        from eps_ranked
        left join chats
            on eps_ranked.user_id = chats.user_id
            and eps_ranked.episode_id = chats.episode_id
            and eps_ranked.resolved_at_day = chats.created_at_day
        where eps_ranked.rank = 1
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
                    first_ep_details.resolved_at_day,
                    first_ep_details.resolved_at_day + interval '90 days'
                ) @>
                timezone('America/Montreal',
                    episodes.first_message_patient)
            and episodes.first_set_resolved_pending_at is not null
        group by 1
    ) 
    
select first_ep_details.*
    , retention_stats.count_episodes
    , users.organization_name
from retention_stats
left join first_ep_details using (user_id)
left join users using (user_id)
where age_in_days > 90
