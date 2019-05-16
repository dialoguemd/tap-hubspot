with
	dxa_cc_manual as (
		select * from {{ ref('dxa_launched_cc') }}
	)

	, labels as (
		select * from {{ ref('dimension_dxa_chief_complaints') }}
	)

	, ccs_parsed_tmp as (
		select * from {{ ref('countdown_chief_complaint_parsed') }}
	)

	, ccs_parsed_rank as (
		select ccs_parsed_tmp.episode_id
			, ccs_parsed_tmp.cc_name as cc_code
			, ccs_parsed_tmp.cc_confidence
			, ccs_parsed_tmp.timestamp
			, labels.description_en as cc_label_en
			, row_number() over (
				partition by ccs_parsed_tmp.episode_id
				order by ccs_parsed_tmp.cc_confidence desc
			) as rank
		from ccs_parsed_tmp
		left join labels
			on ccs_parsed_tmp.cc_name = labels.chief_complaint
	)

	, cc_parsed_first as (
		select episode_id
			, cc_code
			, cc_label_en
			, cc_confidence
			, timestamp
			, timezone('America/Montreal', timestamp) as timestamp_est
		from ccs_parsed_rank
		where rank = 1
	)

	, cc_manual as (
		select dxa_cc_manual.episode_id
			, dxa_cc_manual.cc as cc_code
			, dxa_cc_manual.timestamp
			, labels.description_en as cc_label_en
			, row_number() over (
				partition by dxa_cc_manual.episode_id
				order by dxa_cc_manual.timestamp
			) as rank
		from dxa_cc_manual
		-- Inner join to exclude CCs that are not in our dimension model
		inner join labels
			on dxa_cc_manual.cc = labels.chief_complaint
	)

	, cc_manual_first as (
		select episode_id
			, cc_code
			, cc_label_en
			, timestamp
			, timezone('America/Montreal', timestamp) as timestamp_est
		from cc_manual
		-- Take the first CC because it is assumed this is the corrected choice
		-- Very few episodes (0.5% as verified on Feb 22, 2019) have multiple valid CCs anyways
		where rank = 1
	)

select
	{%
		for col in ['episode_id', 'timestamp', 'timestamp_est', 'cc_code',
		'cc_label_en']
	%}

	{% if not loop.first %}, {% endif %}

	coalesce(cc_parsed_first.{{col}}, cc_manual_first.{{col}}) as {{col}}

{% endfor %}

	, cc_parsed_first.cc_code as cc_code_parsed
	, cc_parsed_first.cc_confidence as cc_code_parsed_confidence
	, cc_manual_first.cc_code as cc_code_manual

	, cc_parsed_first.timestamp_est as cc_code_parsed_timestamp_est
	, cc_manual_first.timestamp_est as cc_code_manual_timestamp_est

	, case
		when cc_parsed_first.episode_id is not null
			and cc_manual_first.episode_id is null
		then 'Triggered automatically'
		when cc_parsed_first.episode_id is null
			and cc_manual_first.episode_id is not null
		then 'Triggered manually'
		when cc_parsed_first.episode_id is not null
			and cc_manual_first.episode_id is not null
		then 'Triggered automatically and manually'
		else 'N/A'
	end
		as dxa_trigger_type

from cc_parsed_first
full outer join cc_manual_first
	using (episode_id)
