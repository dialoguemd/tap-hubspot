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
	, organizations.billing_start_date
	, coalesce(
		organizations.billing_start_date <> '1970-01-01'
		and plans.charge_price <> 0, false) as is_paid
	, organizations.email_preference
	, organization_address_unique.city
	, case
		when organizations.billing_start_date = '1970-01-01'
		then 'free'
		else plans.charge_strategy
	end as charge_strategy
	, case
		when organizations.billing_start_date = '1970-01-01'
		then 0
		else plans.charge_price
	end as charge_price
	, plans.charge_price_mental_health
	, plans.charge_price_24_7
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
from organizations
left join organization_address_unique
	using (organization_id)
inner join plans
	using (organization_id)
left join test_organizations
	using (organization_id)
where test_organizations.organization_id is null
