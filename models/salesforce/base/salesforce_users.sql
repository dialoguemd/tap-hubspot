select *
	, case
        when name = 'Jonathan Bolduc'
        then '2017-12-01'
        else created_date
      end as started_date
    , id as user_id
    , name as user_name
    , state_code as province
from salesforce.users
