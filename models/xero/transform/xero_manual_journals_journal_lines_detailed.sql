with
	manual_journals as (
		select * from {{ ref('xero_manual_journals') }}
	)

	, line_items as (
		select * from {{ ref('xero_manual_journals_journal_lines') }}
	)

select *
	, case
		when manual_journals.line_amount_types = 'Inclusive'
		then line_items.line_amount - line_items.tax_amount
		else line_items.line_amount
	end as line_amount_excl_tax
from manual_journals
inner join line_items
	using (manual_journal_id)
where manual_journals.status not in ('VOIDED')
