select code as account_code
	, name as account_name
	, type as account_type
	, updateddateutc as updated_date_utc
	, reportingcodename as reporting_code_name
	, systemaccount as system_account
	, bankaccounttype as bank_account_type
	, taxtype as tax_type
	, description as account_description
	, class as account_class
	, accountid as account_id
	, bankaccountnumber as bank_account_number
	, status
	, showinexpenseclaims as show_in_expense_claims
	, currencycode as currency_code
	, reportingcode as reporting_code
	, enablepaymentstoaccount as enable_payments_to_account
	, hasattachments as has_attachments
from tap_xero.accounts
