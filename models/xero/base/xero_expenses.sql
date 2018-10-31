select *
	, case
		when line_amount_types = 'Inclusive'
		then line_amount - tax_amount
		else line_amount
	end as line_amount_excl_tax
from xero.expenses
