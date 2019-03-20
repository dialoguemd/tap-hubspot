with
	user_contract as (
		select * from {{ ref('scribe_user_contract') }}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts_detailed') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

	, users as (
		select * from {{ ref('scribe_users_detailed') }}
	)

	, dimension_province as (
		select * from {{ ref('dimension_province') }}
	)

	, detailed as (
		select contracts.contract_id
			, user_contract.user_id
			, contracts.organization_id
			, contracts.participant_id
			, contracts.charge_price
			, contracts.charge_price_mental_health
			, contracts.charge_price_24_7
			, contracts.charge_strategy
			, date_trunc('month', contracts.during_start) as invited_month
            , date_trunc('month', users.signed_up_at) as signed_up_month

			-- Organize during fields for filtering out overlapping contracts
			, contracts.during_start
			, contracts.during_end

			, organizations.billing_start_date
			, organizations.organization_name
			, organizations.is_paid as organization_is_paid
			, user_contract.user_id = contracts.participant_id as is_employee

			-- If available use the 1) user-selected province, if not then the
			-- 2) admin-set user-level province, or if neither are availble use
			-- the 3) org's province
			, coalesce(
				users.residence_province,
				contracts.admin_area_name,
				organizations.province
				) as residence_province

			, users.country
			, users.birthday
			, users.gender
			, users.is_child
				-- case when the user was added as a child but is more than 14 years old
					and (
						users.birthday is null
						or users.birthday < contracts.during_start - interval '1 year' * 14
					)
			as is_child
			, case
				when user_contract.user_id = contracts.participant_id
				then 'Employee'
				when users.is_child
				-- case when the user was added as a child but is more than 14 years old
					and (
						users.birthday is null
						or extract('year' from age(contracts.during_start, users.birthday)) < 14
					)
				then 'Child'
				else 'Dependent'
			end as family_member_type
			, users.signed_up_at
			, users.is_signed_up
			, case
				when users.language <> 'N/A' then users.language
		        when organizations.email_preference in ('bilingual-french-english', 'french')
		        then 'FR'
		        when organizations.email_preference in ('bilingual-english-french', 'english')
		        then 'EN'
		      end as language
		from user_contract
		inner join contracts
			using (contract_id)
		inner join organizations
			using (organization_id)
		inner join users
			using (user_id)
	)

select detailed.contract_id
	, detailed.user_id
	, detailed.organization_id
	, detailed.participant_id
	, detailed.charge_price
	, detailed.charge_price_mental_health
	, detailed.charge_price_24_7
	, detailed.charge_strategy
	, detailed.during_start
	, detailed.during_end
	-- construct during tsranges for tstz and est-specific
	, tstzrange(
			detailed.during_start,
			detailed.during_end
		) as during
	, tsrange(
			timezone('America/Montreal', detailed.during_start),
			timezone('America/Montreal', detailed.during_end)
		) as during_est
	, detailed.billing_start_date
	, detailed.organization_name
	, detailed.organization_is_paid
	, detailed.is_employee
	, detailed.residence_province
	, detailed.country
	, detailed.birthday
	, detailed.gender
	, detailed.is_child
	, detailed.family_member_type
	, detailed.signed_up_at
	, detailed.is_signed_up
	, detailed.language
	, detailed.invited_month
	, detailed.signed_up_month
	, dimension_province.province_code as residence_province_code
	, dimension_province.region as residence_region
from detailed
left join dimension_province
	on detailed.residence_province = dimension_province.province_name
