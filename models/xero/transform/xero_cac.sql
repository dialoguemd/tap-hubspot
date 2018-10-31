with
	eae_headcount_ratio_monthly as (
		select * from {{ ref('salesforce_eae_headcount_ratio_monthly') }}
	)

	, data_sales_cost_split_monthly as (
		select * from {{ ref('data_sales_cost_split_monthly') }}
	)

	, xero_profit_and_loss as (
		select * from {{ ref('xero_profit_and_loss') }}
	)

	, sales_cost_split_monthly as (
		select date_month
			, coalesce(
				sum(wage_percentage) filter(where
					province = 'QC'
					and title = 'Enterprise Account Executive'
				)
				, 0
			) as eae_qc_perc
			, coalesce(
				sum(wage_percentage) filter(where
					province <> 'QC'
					and title = 'Enterprise Account Executive'
				)
				, 0
			  ) as eae_roc_perc
			  , coalesce(
				sum(wage_percentage) filter(where
					province = 'QC'
					and title = 'Account Executive'
				)
				, 0
			  ) as ae_qc_perc
			  , coalesce(
				sum(wage_percentage) filter(where
					province <> 'QC'
					and title = 'Account Executive'
				)
				, 0
			  ) as ae_roc_perc
		from data_sales_cost_split_monthly
		group by 1
	)

	, pl_rate_monthly as (
		select xero_profit_and_loss.*
			, coalesce(case
				when xero_profit_and_loss.account_cost_category in (
					'Sales and Marketing - Professional Services',
					'Sales and Marketing - Marketing Campaign',
					'Sales and Marketing - SAAS',
					'Sales & Marketing - Other'
				) then coalesce(eae_headcount_ratio_monthly.eae_qc_perc, .6)
				when xero_profit_and_loss.account_cost_category = 'Sales and Marketing - Wages'
				then coalesce(sales_cost_split_monthly.eae_qc_perc, .6)
			end, 0) as eae_qc_perc
			, coalesce(case
				when xero_profit_and_loss.account_cost_category in (
					'Sales and Marketing - Professional Services',
					'Sales and Marketing - Marketing Campaign',
					'Sales and Marketing - SAAS',
					'Sales & Marketing - Other'
				) then coalesce(eae_headcount_ratio_monthly.eae_roc_perc, 0.05)
				when xero_profit_and_loss.account_cost_category = 'Sales and Marketing - Wages'
				then coalesce(sales_cost_split_monthly.eae_roc_perc, .05)
			end, 0) as eae_roc_perc
			, coalesce(case
				when xero_profit_and_loss.account_cost_category in (
					'Sales and Marketing - Professional Services',
					'Sales and Marketing - Marketing Campaign',
					'Sales and Marketing - SAAS',
					'Sales & Marketing - Other'
				) then coalesce(eae_headcount_ratio_monthly.ae_qc_perc, .35)
				when xero_profit_and_loss.account_cost_category = 'Sales and Marketing - Wages'
				then coalesce(sales_cost_split_monthly.ae_qc_perc, .35)
			end, 0) as ae_qc_perc
			, coalesce(case
			when xero_profit_and_loss.account_cost_category in (
				'Sales and Marketing - Professional Services',
				'Sales and Marketing - Marketing Campaign',
				'Sales and Marketing - SAAS',
				'Sales & Marketing - Other'
			) then eae_headcount_ratio_monthly.ae_roc_perc
			when xero_profit_and_loss.account_cost_category = 'Sales and Marketing - Wages'
			then sales_cost_split_monthly.ae_roc_perc
			end, 0) as ae_roc_perc
		from xero_profit_and_loss
		left join eae_headcount_ratio_monthly
			using (date_month)
		left join sales_cost_split_monthly
			on xero_profit_and_loss.date_month = sales_cost_split_monthly.date_month
		where xero_profit_and_loss.account_group in ('Sales Expenses', 'Marketing Expenses')
	)

select date_month
	, sum(eae_qc_perc * amount_excl_tax) as cost_eae_qc
	, sum(eae_roc_perc * amount_excl_tax) as cost_eae_roc
	, sum(ae_qc_perc * amount_excl_tax) as cost_ae_qc
	, sum(ae_roc_perc * amount_excl_tax) as cost_ae_roc
	, sum(amount_excl_tax) as cost_total
from pl_rate_monthly
group by 1
