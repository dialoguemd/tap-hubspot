with links as (
		select * from {{ ref( 'zendesk_links' ) }}
	)

select *
from links
where rank = 1
