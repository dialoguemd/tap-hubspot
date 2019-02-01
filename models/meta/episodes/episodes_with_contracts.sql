with episodes as (
		select * from {{ ref('episodes') }}
	)

	, user_contracts as (
		select * from {{ ref('user_contract') }}
	)

select episodes.*
	, user_contracts.family_member_type
	, user_contracts.organization_name
	, user_contracts.organization_id
	, user_contracts.account_name
	, user_contracts.account_id
from episodes
-- TODO change from inner to left join and verify that all episodes join to a
-- valid user_contract
inner join user_contracts
	on episodes.patient_id = user_contracts.user_id
	and episodes.first_message_created_at
		<@ user_contracts.during_est
