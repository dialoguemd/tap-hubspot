with
	invoices as (
		select * from {{ ref('xero_invoices') }}
	)

	, line_items as (
		select * from {{ ref('xero_invoices_line_items') }}
	)

select *
	, case
		when invoices.invoice_type = 'ACCPAY'
		then 1
		else -1
	end
	* case
		when invoices.line_amount_types = 'Inclusive'
		then line_items.line_amount - line_items.tax_amount
		else line_items.line_amount
	end as line_amount_excl_tax
from invoices
inner join line_items
	using (invoice_id)
where invoices.status not in ('VOIDED', 'DELETED')
