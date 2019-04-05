with
	active_users as (
		select * from {{ ref('messaging_posts_patient_daily') }}
	)

select user_id
	, min(first_message_created_at) as activated_at
from active_users
group by 1
