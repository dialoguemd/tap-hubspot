with
	ratings as (
		select * from {{ ref('episodes_ratings') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select ratings.*
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.account_id
	, user_contract.account_name
from ratings
inner join user_contract
	on ratings.user_id = user_contract.user_id
	and ratings.timestamp <@ user_contract.during
