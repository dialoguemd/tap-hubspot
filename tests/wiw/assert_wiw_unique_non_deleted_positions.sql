with
	positions as (
		select * from {{ ref('wiw_positions') }}
	)

select name
	, count(*)
from positions
where not is_deleted
group by 1
having count(*) > 1
