with
	active_users as (
		select * from {{ ref('active_users') }}
	)

	, login_fetch_complete as (
		select * from {{ ref('patientapp_login_fetch_complete') }}
	)


select date_day
	, count(distinct active_users.user_id) as active
	, count(distinct login_fetch_complete.user_id) as login
	, count(distinct login_fetch_complete.user_id)  * 1.0
	/ count(distinct active_users.user_id) as fraction
from active_users
left join login_fetch_complete
	on active_users.user_id = login_fetch_complete.user_id
	and active_users.date_day = date_trunc('day', login_fetch_complete.timestamp)
group by 1
