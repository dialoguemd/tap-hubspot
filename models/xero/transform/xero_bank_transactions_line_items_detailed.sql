with
	bank_transactions as (
		select * from {{ ref('xero_bank_transactions') }}
	)

	, line_items as (
		select * from {{ ref('xero_bank_transactions_line_items') }}
	)

select *
	, case
		when bank_transactions.bank_transaction_type = 'RECEIVE'
		then -1
		else 1
	end
	* case
		when bank_transactions.line_amount_types = 'Inclusive'
		then line_items.line_amount - line_items.tax_amount
		else line_items.line_amount
	end as line_amount_excl_tax
from bank_transactions
inner join line_items
	using (bank_transaction_id)
where bank_transactions.status not in ('DELETED')
