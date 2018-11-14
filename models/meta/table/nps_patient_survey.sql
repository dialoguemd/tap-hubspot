with
	nps_survey as (
		select * from {{ ref('nps_patient_survey_with_user_id') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract')}}
	)

select nps_survey.*
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.family_member_type
	, user_contract.is_employee
	, user_contract.is_child
	, user_contract.residence_province
	, user_contract.country
	, user_contract.gender
	, user_contract.language
from nps_survey
inner join user_contract
	on nps_survey.user_id = user_contract.user_id
		and nps_survey.received_at <@ user_contract.during
