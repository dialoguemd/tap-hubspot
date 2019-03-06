with
	command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

select episode_id
	, user_id
	, timestamp
from command_triggered
where command_name = 'templates'
	and command_id = 'Appointment Virtual'
