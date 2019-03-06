with
	organization_address as (
		select * from {{ ref('scribe_organization_address') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations') }}
	)

	, test_organizations as (
		select * from {{ ref('scribe_test_organizations') }}
	)

	, plans as (
		select * from {{ ref('scribe_plans_detailed') }}
	)

	, organizations_contracts as (
		select * from {{ ref('scribe_organizations_contracts') }}
	)

	, organizations_billing_start_date as (
		select * from {{ ref('scribe_organizations_billing_start_date') }}
	)

	, organization_address_rank as (
		select organization_id
			, organization_address_id
			, city
			, row_number() over (
				partition by organization_id
				order by organization_address_id desc
			) as rank
		from organization_address
	)

	, organization_address_unique as (
		select organization_id
			, city
		from organization_address_rank
		where rank = 1
	)

select organizations.organization_id
	, organizations.organization_name
	, coalesce(
		organizations_billing_start_date.billing_start_date
		, organizations.billing_start_date
	) as billing_start_date
	, coalesce(plans.charge_price, 0) <> 0 as is_paid
	, organizations.email_preference
	, organization_address_unique.city
	, case
		when coalesce(plans.charge_price, 0) = 0
		then 'free'
		else plans.charge_strategy
	end as charge_strategy
	, coalesce(plans.charge_price, 0) as charge_price
	, plans.charge_price_mental_health
	, plans.charge_price_24_7
	, coalesce(plans.features, 'family') as features
	, coalesce(plans.has_mental_health, false) as has_mental_health
	, coalesce(plans.has_24_7, false) as has_24_7
	, case 
		when organization_name like '%Toronto'
			or organization_name like '%Ontario'
			or organization_name like '%ON'
			or organization_name like '%(Ontario)'
			or organization_name like '%(Toronto)'
			or organization_name like '%ON) '
			or organization_name like '%ON)'
			or organization_name like '%Ottawa%'
			or organization_name like '%(Ottawa)'
		then 'Ontario'
		when organization_name like '%Vancouver'
			or organization_name like '%British Columbia'
			or organization_name like '%BC'
			or organization_name like '%BC '
			or organization_name like '%British-Columbia'
			or organization_name like '%Colombie-Britannique'
			or organization_name like '%(British-Columbia)'
			or organization_name like '%(British Columbia)'
			or organization_name like '%(BC)'
			or organization_name like '%(Vancouver)'
			or organization_name like '%BC) '
		then 'British Columbia'
		when organization_name like '%Nova Scotia'
			or organization_name like '%Halifax'
			or organization_name like '%NS'
			or organization_name like '%(Nova Scotia)'
			or organization_name like '%(Halifax)'
		then 'Nova Scotia'
		when organization_name like '%Alberta'
			or organization_name like '%AB'
			or organization_name like '%Calgary'
			or organization_name like '%(Alberta)'
			or organization_name like '%(Calgary)'
		then 'Alberta'
		when city in ('Montreal', 'Laval', ' Dollard-Des Ormeaux', 'Montréal', 'Quebec',
			'Terrebonne', 'Verdun', 'Québec') then 'Quebec'
		when city = 'Toronto' then 'Ontario'
		when organization_name like '%Yukon' then 'Yukon'
		when tax_province = 'QC' then 'Quebec'
		when tax_province = 'ON' then 'Ontario'
		when tax_province = 'MB' then 'Manitoba'
		when tax_province = 'YT' then 'Yukon'
		when tax_province = 'AB' then 'Alberta'
		when tax_province = 'SK' then 'Saskatchewan'
		when tax_province = 'BC' then 'British Columbia'
		when tax_province = 'NB' then 'New Brunswick'
		when tax_province = 'NS' then 'Nova Scotia'
		when tax_province = 'NL' then 'Newfoundland and Labrador'
		when tax_province is not null then tax_province
		else 'Quebec'
	end as province
	, organizations_contracts.first_contract_start_date
	, organizations_contracts.last_contract_end_date
	, coalesce(organizations_contracts.is_churned, false) as is_churned
	-- Activated organizations have at least one contract
	, organizations_contracts.organization_id is null as is_activated
from organizations
left join organization_address_unique
	using (organization_id)
inner join plans
	using (organization_id)
left join organizations_contracts
	using (organization_id)
left join organizations_billing_start_date
	using (organization_id)
left join test_organizations
	using (organization_id)
where test_organizations.organization_id is null
