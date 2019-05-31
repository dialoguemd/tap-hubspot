select description
	, quantity
	, unitamount as unit_amount
	, accountcode as account_code
	, itemcode as item_code
	, lineitemid as line_item_id
	, taxtype as tax_type
	, lineamount as line_amount
	, taxamount as tax_amount
	, discountrate as discount_rate
	, _sdc_source_key_invoiceid as invoice_id
from tap_xero.invoices__lineitems
