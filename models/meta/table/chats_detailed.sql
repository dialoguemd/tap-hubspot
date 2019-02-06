with
	chats as (
		select * from {{ ref('chats') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select chats.*
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.residence_province
from chats
inner join user_contract
	on chats.patient_id = user_contract.user_id
		and chats.first_message_patient <@ user_contract.during_est
