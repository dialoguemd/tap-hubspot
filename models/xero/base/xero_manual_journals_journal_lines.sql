select lineamount as line_amount
	, description
	, taxamount as tax_amount
	, accountid as account_id
	, accountcode as account_code
	, isblank as is_blank
	, taxtype as tax_type
	, _sdc_source_key_manualjournalid as manual_journal_id
from tap_xero.manual_journals__journallines
