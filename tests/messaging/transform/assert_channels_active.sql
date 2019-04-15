-- Test the percentage of channels created that were never set to active
-- by usher

with
	messaging_channels as (
		select * from {{ ref('messaging_channels') }}
	)

	, episode_state_summary as (
		select * from {{ ref('usher_episode_state_summary') }}
	)

select date_trunc('week', messaging_channels.created_at)
    , 1.0 * count(*) filter(where episode_state_summary.episode_id is null)
        / count(*)
from messaging_channels
left join episode_state_summary
    using (episode_id)
where messaging_channels.created_at > '2018-01-01'
    and messaging_channels.created_at < date_trunc('week', current_date)
group by 1
-- Calibrated in April 2019
having 1.0 * count(*) filter(where episode_state_summary.episode_id is null)
        / count(*) > .25
