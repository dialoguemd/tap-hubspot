with
	dxa_cc as (
		select * from {{ ref( 'messaging_dxa_cc' ) }}
	)

	, cc as (
		select episode_id
			, cc as cc_code
			, row_number() over (partition by episode_id order by timestamp)
				as rank
		from dxa_cc
	)

select episode_id
	, cc_code
from cc
where rank = 1
