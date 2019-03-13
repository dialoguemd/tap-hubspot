
{{ config(materialized='table') }}

with
	command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

select episode_id
	, user_id
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
	, row_number() over (partition by episode_id order by timestamp) as rank
from command_triggered
where command_name = 'templates'
	and command_id = 'Appointment Virtual'
