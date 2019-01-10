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
			, lag(contracts.during_start) over
				(partition by user_contract.user_id
					order by contracts.during_start, contracts.contract_id)
					as previous_during_start
			, lag(contracts.during_end) over
				(partition by user_contract.user_id
					order by contracts.during_start, contracts.contract_id)
					as previous_during_end

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

select contract_id
	, user_id
	, organization_id
	, participant_id
	, charge_price
	, charge_price_mental_health
	, charge_price_24_7
	, charge_strategy
	-- correct during_start to eliminate overlap
	-- TODO: add a data test to test all known use cases
	, case when previous_during_start is null then during_start
		when during_start < previous_during_end then previous_during_end
		else during_start end as during_start
	, during_end
	-- construct during tsranges for tstz and est-specific
	, tstzrange(
			case when previous_during_start is null then during_start
				when during_start < previous_during_end then previous_during_end
				else during_start end,
			during_end
		) as during
	, tsrange(
			timezone('America/Montreal', 
				case when previous_during_start is null then during_start
					when during_start < previous_during_end then previous_during_end
					else during_start end),
			timezone('America/Montreal', during_end)
		) as during_est
	, billing_start_date
	, organization_name
	, organization_is_paid
	, is_employee
	, residence_province
	, country
	, birthday
	, gender
	, is_child
	, family_member_type
	, signed_up_at
	, is_signed_up
	, language
	, invited_month
	, signed_up_month
from detailed
-- Filter out all user_contracts that have a previous_during_end that is 
-- greater than its during end (i.e. the contract in question starts after
-- the previous contract and ends before the previous contract)
where (during_end > previous_during_end
	or previous_during_end is null)
