with
	pipeline_monthly as (
		select * from {{ ref('salesforce_pipeline_monthly') }}
	)

	, probabilities as (
		select * from {{ ref('salesforce_pipeline_probabilities')}}
	)

	select pipeline_monthly.*
	    , pipeline_monthly.amount * coalesce(probabilities.probability, 1) as amount_weighted
	    , case stage_name_this_month
			when 'Decide' then 0.9
			when 'Justify' then 0.75
			when 'Validate' then 0.5
			when 'Educate' then 0.25
			when 'Initiate' then 0.1
			else 0.01
		end * pipeline_monthly.amount as amount_sf_weighted
	from pipeline_monthly
	left join probabilities
		on pipeline_monthly.segment = probabilities.segment
			and pipeline_monthly.stage_name_this_month = probabilities.stage_name
