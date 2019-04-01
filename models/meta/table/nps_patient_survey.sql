with
	nps_survey as (
		select * from {{ ref('nps_patient_survey_with_user_id') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract')}}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

select md5(nps_survey.survey_id || user_contract.contract_id) as survey_id
	, nps_survey.episode_id
	, nps_survey.score
	, nps_survey.category
	, nps_survey.tags
	, nps_survey.comment
	, nps_survey.timestamp
	, nps_survey.updated_at
	, nps_survey.user_id
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.family_member_type
	, user_contract.is_employee
	, user_contract.is_child
	, user_contract.residence_province
	, user_contract.residence_province_code
	, user_contract.residence_region
	, user_contract.country
	, user_contract.gender
	, user_contract.language
	, episodes.outcome as episode_outcome
	, episodes.issue_type as episode_issue_type
from nps_survey
inner join user_contract
	on nps_survey.user_id = user_contract.user_id
		and nps_survey.timestamp <@ user_contract.during
left join episodes
	using (episode_id)
