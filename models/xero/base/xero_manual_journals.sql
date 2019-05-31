select {{ expand_date(ts_field='date') }}
	, lineamounttypes as line_amount_types
	, status
	, narration
	, url
	, showoncashbasisreports
	, hasattachments
	, updateddateutc as updated_date
	, manualjournalid as manual_journal_id
from tap_xero.manual_journals
