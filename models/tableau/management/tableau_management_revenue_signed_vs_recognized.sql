with accounts_monthly as (
		select *
		from {{ ref('meta_accounts_monthly') }}
		where date_month >= '2018-09-01'
			and date_month < current_date - interval '1 month'
	)

	, sf_opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, sf_opportunities_won as (
		select account_id
			, account_name
			, min(date_trunc('month', close_date)) as close_date
			, min(coalesce(billing_start_date, i_date)) as billing_start_date
			, sum(number_of_employees) as number_of_employees
			, sum(amount) as amount
			, count(*) as opps_won
		from sf_opportunities
		where is_won
			and '01t6A000002NnLXQA0' = any(product_ids)
			and value_period = 'Monthly'
			and not is_pilot
		group by 1,2
		having min(coalesce(billing_start_date, i_date)) is not null
	)

	, sf_opportunities_won_monthly as (
		select *
			, date_trunc('month', sf_opportunities_won_monthly.billing_start_date)
				as billing_start_month
			, generate_series(greatest('2018-09-01', close_date)
				, date_trunc('month', current_date) - interval '1 month'
				, interval '1 month') as date_month
		from sf_opportunities_won
	)

	, signed_vs_recognized as (
		select coalesce(sf_opportunities_won_monthly.account_id
				, accounts_monthly.account_id
			) as account_id
			, coalesce(sf_opportunities_won_monthly.account_name
				, accounts_monthly.account_name
			) as account_name
			, coalesce(sf_opportunities_won_monthly.date_month
				, accounts_monthly.date_month
			) as date_month
			, sf_opportunities_won_monthly.billing_start_date as billing_start_date_sf
			, accounts_monthly.billing_start_date as billing_start_date_accounts
			, case
				when sf_opportunities_won_monthly.account_id is null
				then 'Launched'
				when sf_opportunities_won_monthly.billing_start_month
					> sf_opportunities_won_monthly.date_month
				then 'Future launch'
				when sf_opportunities_won_monthly.billing_start_month
					= sf_opportunities_won_monthly.date_month
					and coalesce(accounts_monthly.price_monthly, 0) > 0
				then 'Launched this month'
				when coalesce(accounts_monthly.price_monthly, 0) = 0
					and (
						sf_opportunities_won_monthly.billing_start_month > date_month
						or accounts_monthly.billing_start_date is null
					)
				then 'Not launched'
				when sf_opportunities_won_monthly.amount > 2000
					and 0.9 * sf_opportunities_won_monthly.amount
						> coalesce(accounts_monthly.price_monthly, 0)
				then 'Not fully launched'
				else 'Launched'
			 end as status
			 , coalesce(sf_opportunities_won_monthly.amount, 0) as sf_amount
			 , coalesce(accounts_monthly.price_monthly, 0) as scribe_price_monthly
			 , coalesce(sf_opportunities_won_monthly.number_of_employees, 0) as sf_number_of_employees
			 , coalesce(accounts_monthly.paid_employees, 0) as scribe_paid_employees
			 , coalesce(sf_opportunities_won_monthly.opps_won, 0) as opps_won
		from sf_opportunities_won_monthly
		full outer join accounts_monthly
			using (account_id, date_month)
	)

select *
	, case
		when status = 'Launched'
		then 0
		when status in ('Not fully launched', 'Launched this month')
			and sf_amount > scribe_price_monthly
		then sf_amount - scribe_price_monthly
		when status = 'Future launch'
		then sf_amount
	end as amount_to_recognize
	, case
		when status in ('Launched', 'Not fully launched', 'Launched this month')
		then scribe_price_monthly
		when status = 'Future launch'
		then 0
	end as amount_recognized
from signed_vs_recognized
