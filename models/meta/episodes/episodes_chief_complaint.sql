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
	)

select cc.episode_id
	, cc.cc_code
	, labels.chief_complaint is not null as is_valid_cc
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
from cc
left join labels
	on cc.cc_code = labels.chief_complaint
where rank = 1
