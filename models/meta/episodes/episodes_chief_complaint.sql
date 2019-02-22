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
			, row_number() over (partition by episode_id order by timestamp desc)
				as rank_desc
		from dxa_cc
		-- Inner join to exclude CCs that are not in our dimension model
		inner join labels
			on dxa_cc.cc = labels.chief_complaint
	)

select cc.episode_id
	, cc.cc_code
	, labels.description_en as cc_label_en
	, labels.chief_complaint is not null as is_valid_cc
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
from cc
-- Take the last CC because it is assumed this is the corrected choice
-- Very few episodes (0.5% as verified on Feb 22, 2019) have multiple valid CCs anyways
where rank_desc = 1
