with
	calls as (
		select * from {{ ref('telephone_calls') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, episodes_issue_types as (
		select * from {{ ref('episodes_issue_types') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners')}}
	)

	, test_users as (
		select * from {{ ref('scribe_test_users')}}
	)

select calls.user_id as practitioner_id
	, episodes_subject.episode_subject as patient_id
	, calls.call_id
	, calls.date_day
	, calls.started_at
	, calls.started_at_est
	, calls.ended_at
	, calls.ended_at_est
	, calls.episode_id
	, calls.call_duration
	, coalesce(practitioners.main_specialization, 'N/A') as main_specialization
	, coalesce(practitioners.user_name, 'N/A') as user_name
	, episodes_issue_types.issue_type
from calls
left join practitioners
	using (user_id)
left join episodes_subject
	using (episode_id)
left join episodes_issue_types
	using (episode_id)
left join test_users
	on episodes_subject.episode_subject = test_users.user_id
where test_users.user_id is null
