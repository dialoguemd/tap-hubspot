with
	chats as (
		select * from {{ ref('chats') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, accounts as (
		select * from {{ ref('accounts') }}
	)

	, accounts_revenue_last_month as (
		select * from {{ ref('finance_accounts_revenue_last_month') }}
	)

	, user_contract_since_start as (
		select user_contract.contract_id
			, user_contract.account_id
			, user_contract.account_name
		-- get percentage of time since billing start date covered by this
		-- contract
			, extract(epoch from
				least(current_timestamp, user_contract.during_end)
					- greatest(
						accounts.billing_start_date
						, user_contract.during_start
					)
			) / extract(epoch from
				current_timestamp - accounts.billing_start_date
			) as overlap_percentage
		from user_contract
		inner join accounts
		    using (account_id)
		where is_employee
			and not accounts.is_churned
	)

	, accounts_contracts as (
		select user_contract_since_start.account_id
			, user_contract_since_start.account_name
			, accounts_revenue_last_month.amount as mrr_last_month
			, sum(user_contract_since_start.overlap_percentage)
				as contracts_avg
		from user_contract_since_start
		left join accounts_revenue_last_month
			using (account_id)
		group by 1,2,3
	)

	, account_health as (
		select user_contract.account_name
			, accounts.billing_start_date
			, accounts_contracts.contracts_avg
			, accounts_contracts.mrr_last_month
			, case
				when accounts_contracts.contracts_avg < 40
				then '1-39'
				when accounts_contracts.contracts_avg < 100
				then '40-99'
				when accounts_contracts.contracts_avg < 200
				then '100-199'
				when accounts_contracts.contracts_avg < 500
				then '200-499'
				when accounts_contracts.contracts_avg < 1000
				then '500-1000'
				else '1000+'
			end as account_segment
			, extract(epoch from current_date - accounts.billing_start_date)
				/ 3600.0 / 24 / 30 as month_since_billing_start_date
			, count(distinct
				chats.date_day_est
				|| coalesce(episodes_subject.episode_subject, chats.user_id)
			) as daus
		from chats
		inner join user_contract
		    on chats.user_id = user_contract.user_id
		        and chats.first_message_patient <@ user_contract.during_est
		left join episodes_subject
			using (episode_id)
		inner join accounts
			using (account_id)
		inner join accounts_contracts
			using (account_id)
		where accounts.billing_start_date < current_timestamp
		group by 1,2,3,4
	)

select *
	, daus / month_since_billing_start_date / contracts_avg
		as utilization_monthly
	-- group accounts above 30% for histogram in Tableau
	, least(.301,
		daus / month_since_billing_start_date / contracts_avg
	) as utilization_monthly_maxed
from account_health
