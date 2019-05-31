with
	payments as (
		select * from {{ ref('xero_payments') }}
	)

select *
	, case
		when payment_type = 'ACCPAYPAYMENT'
		then -1
		else 1
	end
	* (line_amount - coalesce(tax_amount, 0))
	as line_amount_excl_tax
from payments
where status not in ('DELETED')
