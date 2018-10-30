select *
	, case
		when account_name in (
			'Sales - Commissions to Third Party',
			'Sales - Software / SAAS',
			'Sales & Partnerships - DAS',
			'Sales & Partnerships - Group Insurance Benefit',
			'Sales & Partnerships - Salaries',
			'Sales - Travel (National)',
			'Sales - Travel (International)',
			'Sales Event',
			'Sales M&E - External',
			'Sales M&E - Internal',
			'Sales Other',
			'Sales Training'
		) then 'Sales Expenses'
    	when account_name in (
    		'Customer Success - DAS',
    		'Customer Success - Group Insurance Benefit',
    		'Customer Success - Salary',
    		'Customer Success - Software / SAAS',
    		'Customer Success - Travel (Internal)'
    	) then 'Customer Success'
	    when account_name in (
	    	'Bank Fees',
	    	'Credit Card Processing Fees',
	    	'Depreciation',
	    	'G&A - Consulting',
	    	'G&A - DAS',
	    	'G&A - Events & Conferences',
	    	'G&A - Finance Consulting',
	    	'G&A - Group Insurance Benefit',
	    	'G&A - HR Consulting',
	    	'G&A - Insurance',
	    	'G&A - Interest',
	    	'G&A - Licences & Permits',
	    	'G&A - M&E Internal',
	    	'G&A - Management Consulting',
	    	'G&A - Office Expenses',
	    	'G&A - Other',
	    	'G&A - Recruitment',
	    	'G&A - Rent',
	    	'G&A - Salaries',
	    	'G&A - Software / SAAS',
	    	'G&A - Travel (International)',
	    	'G&A - Travel (National)',
	    	'G&A SAAS',
	    	'Legal & Accounting firm expenses',
	    	'Payroll Processing Fees',
	    	'Telecommunications',
	    	'Transaction Fee (Pooto)',
	    	'Utilities'
	    ) then 'General Administration'
	    when account_name in (
	    	'Health Team - Medical Director - DAS',
	    	'Health team - Medical director - Group Insurance Benefit',
	    	'Health team - Medical director consultant',
	    	'Health team - Medical director salary',
	    	'Health Team - Software / SAAS',
	    	'Health Team medical operations - DAS',
	    	'Health team medical operations - Group Insurance Benefit',
	    	'Health team medical operations - Salaries',
	    	'Health team medical QA Consulting',
	    	'Healthcare Board Advisor'
	    ) then 'Health Team'
	    when account_name in (
	    	'Marketing - Campaign',
	    	'Marketing - DAS',
	    	'Marketing - Group Insurance Benefit',
	    	'Marketing - Salaries',
	    	'Marketing - Software / SAAS',
	    	'Marketing Consultants',
	    	'Marketing Event',
	    	'Marketing Other',
	    	'Marketing Training'
	    ) then 'Marketing Expenses'
	    when account_name in (
	    	'Product & Tech - DAS',
	    	'Product & Tech - Group Insurance Benefit',
	    	'Product & Tech - Salaries',
	    	'Technology - Hardware',
	    	'Technology - Infrastructure',
	    	'Technology Advisor Consultant'
	    ) then 'Technology'
	    else 'N/A'
	    end as account_group
	    , case when account_name in (
	    		'Sales - Commissions to Third Party',
	   			'Sales & Partnerships - DAS',
	   			'Sales & Partnerships - Group Insurance Benefit',
	   			'Sales & Partnerships - Salaries',
	   			'Marketing - DAS',
	   			'Marketing - Group Insurance Benefit',
	   			'Marketing - Salaries'
   			) then 'Sales and Marketing - Wages'
	   		when account_name in (
   				'Sales - Travel (National)',
				'Sales Event',
				'Sales M&E - External',
				'Sales M&E - Internal',
				'Sales Other',
				'Sales - Travel (International)'
	   		) then 'Sales & Marketing - Other'
	   		when account_name in (
   				'Marketing - Software / SAAS',
				'Sales - Software / SAAS'
	   		) then 'Sales and Marketing - SAAS'
	   		when account_name in (
				'Marketing Consultants',
				'Marketing Event',
				'Marketing Other',
				'Sales Training',
				'Marketing Training'
	   		) then 'Sales and Marketing - Professional Services'
	   		when account_name = 'Marketing - Campaign'
	   		then 'Sales and Marketing - Marketing Campaign'
	   		end as cost_category
	    , case
	    	when line_amount_types = 'Inclusive'
    		then line_amount - tax_amount
    		else line_amount
	    end as line_amount_excl_tax
from xero.expenses
