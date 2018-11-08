with
	user_contract as (
		select * from {{ ref('scribe_user_contract') }}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

	, users as (
		select * from {{ ref('scribe_users_detailed') }}
	)

select contracts.contract_id
	, user_contract.user_id
	, contracts.organization_id
	, contracts.participant_id
	, contracts.during
	, organizations.billing_start_date
	, organizations.organization_name
	, organizations.is_paid as organization_is_paid
	, user_contract.user_id = contracts.participant_id as is_employee
	, users.residence_province
	, users.country
	, users.birthday
	, users.gender
	, users.is_child
		-- case when the user was added as a child but is more than 14 years old
			and (
				users.birthday is null
				or users.birthday < lower(contracts.during) - interval '1 year' * 14
			)
	as is_child
	, case
		when user_contract.user_id = contracts.participant_id
		then 'employee'
		when users.is_child
		-- case when the user was added as a child but is more than 14 years old
			and (
				users.birthday is null
				or users.birthday < lower(contracts.during) - interval '1 year' * 14
			)
		then 'child'
		else 'dependent'
	end as family_member_type
	, users.signed_up_at
	, users.is_signed_up
	, coalesce(users.language,
      case
        when organizations.email_preference in ('bilingual-french-english', 'french')
        then 'fr'
        when organizations.email_preference in ('bilingual-english-french', 'english')
        then 'en'
      end) as language
from user_contract
inner join contracts
	using (contract_id)
inner join organizations
	using (organization_id)
inner join users
	using (user_id)
