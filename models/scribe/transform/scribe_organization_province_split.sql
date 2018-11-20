with
	user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

select organization_id
    , 1.0 * count(*) filter(where residence_province = 'Quebec')
    	/ count(*) as qc_perc
    , 1.0 * count(*) filter(where residence_province = 'Ontario')
    	/ count(*) as on_perc
    , 1.0 * count(*) filter(where residence_province not in ('Quebec', 'Ontario'))
    	/ count(*) as roc_perc
from user_contract
where is_signed_up
    and is_employee
group by 1
