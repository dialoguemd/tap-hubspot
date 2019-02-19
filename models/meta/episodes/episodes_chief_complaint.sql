with
	dxa_cc as (
		select * from {{ ref('messaging_dxa_cc') }}
	)

	, labels as (
		select * from {{ ref( 'dimension_dxa_chief_complaints' ) }}
	)

	, cc as (
		select episode_id
			, cc as cc_code
			, timestamp
			, row_number() over (partition by episode_id order by timestamp)
				as rank
		from dxa_cc
		-- Inner join to exclude CCs that are not in our dimension model
		inner join labels
			on cc.cc_code = labels.chief_complaint
	)

select cc.episode_id
	, cc.cc_code
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
from cc
where rank = 1
