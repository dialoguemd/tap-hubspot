with
	commands as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

select commands.episode_id
    , commands.command_name
    , commands.user_id
    , commands.command_id
    , commands.triggered_at
    , episodes.patient_id
    , 'https://zorro.dialogue.co/conversations/' || episodes.episode_id
		as url_zorro
from commands
left join episodes
	using (episode_id)
where commands.command_name = 'templates'
	and commands.command_id = 'Bonjour Sante'
