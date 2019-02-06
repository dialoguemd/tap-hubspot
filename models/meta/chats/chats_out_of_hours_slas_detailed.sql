with
	chats_slas as (
		select * from {{ ref('chats_out_of_hours_slas') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select chats_slas.*
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.residence_province
from chats_slas
inner join user_contract
	on chats_slas.patient_id = user_contract.user_id
		and chats_slas.first_message_patient <@ user_contract.during_est
