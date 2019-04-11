with
	churn_monthly as (
		select * from {{ ref('finance_churn_monthly') }}
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, monthly as (
		select churn_monthly.account_id
			, churn_monthly.account_name
			, churn_monthly.amount
			, churn_monthly.date_month
			, churn_monthly.first_month
			, churn_monthly.billing_start_date
			, (
				date_part('year', churn_monthly.date_month)
				- date_part('year', churn_monthly.first_month)
			) * 12
			+ (
				date_part('month', churn_monthly.date_month)
				- date_part('month', churn_monthly.first_month)
			) as months_since_start
			, opportunities.commission_rate_year_1
			, opportunities.commission_rate_year_2
			, opportunities.partner_contact_id
			, opportunities.partner_contact_name
			, opportunities.partner_account_id
			, opportunities.partner_name
			, opportunities.close_date
		from churn_monthly
		inner join opportunities
			using (account_id)
		where opportunities.is_won
	)

select md5(account_id || date_month) as account_month_id
	, partner_account_id
	, partner_name
	, account_id
	, account_name
	, close_date
	, first_month
	, billing_start_date
	, amount
	, date_month
	, partner_contact_id
	, partner_contact_name
	, case
		when months_since_start < 12
		then commission_rate_year_1
		else commission_rate_year_2
	end as commission_percentage
	, case
		when months_since_start < 12
		then commission_rate_year_1
		else commission_rate_year_2
	end * amount as monthly_commission
from monthly
where (
		(
			months_since_start < 12
			and commission_rate_year_1 is not null
			and commission_rate_year_1 <> 0
		) or (
			months_since_start >= 12
			and commission_rate_year_2 is not null
			and commission_rate_year_2 <> 0
		)
	)
	-- exclude the partner account as they do not get commission on it
	and partner_account_id <> account_id
