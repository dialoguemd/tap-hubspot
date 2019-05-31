{% set columns = [
	'account_code', 'line_amount_excl_tax', 'line_amount', 'line_amount_types',
	'tax_amount', 'tax_type','date_day', 'date_week', 'date_month',
	'account_name', 'account_description'
] %}

with
	bt_lines as (
		select * from {{ ref('xero_bank_transactions_line_items_detailed') }}
	)

	, invoice_lines as (
		select * from {{ ref('xero_invoices_line_items_detailed') }}
	)

	, mj_lines as (
		select * from {{ ref('xero_manual_journals_journal_lines_detailed') }}
	)

	, payments as (
		select * from {{ ref('xero_payments_detailed') }}
	)

	, accounts as (
		select * from {{ ref('xero_accounts') }}
	)

{% for table in ['bt_lines', 'invoice_lines', 'mj_lines', 'payments'] %}
	select
		{% for column in columns %}
		
			{% if not loop.first %}
				,
			{% endif %}
		
			{{ column}}
		
		{% endfor %}

		, '{{table}}' as expense_source

	from {{table}}
	left join accounts
		using (account_code)

	{% if not loop.last %}
		union all
	{% endif %}

{% endfor %}
