with
	pipeline_monthly as (
		select * from {{ ref('salesforce_pipeline_monthly') }}
	)

	, probabilities as (
		select * from {{ ref('salesforce_pipeline_probabilities')}}
	)

	, default_probabilities as (
		select * from {{ ref('salesforce_pipeline_probabilities_default')}}
	)

select pipeline_monthly.*
	, pipeline_monthly.amount
		* case
			when pipeline_monthly.segment_group = '500+'
			then default_probabilities.probability
			else coalesce(
				probabilities.probability_opp_3_months,
				default_probabilities.probability
			)
		end
	as amount_weighted
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
	on pipeline_monthly.segment_group = probabilities.segment_group
		and pipeline_monthly.stage_name_this_month = probabilities.stage_name
		and (
			date_trunc('quarter', pipeline_monthly.meeting_date) - interval '6 months'
				= probabilities.date_quarter
			or (
				pipeline_monthly.meeting_date < '2018-06-01'
				and probabilities.date_quarter = '2018-01-01'
			)
		)
left join default_probabilities
	on pipeline_monthly.segment_group = default_probabilities.segment_group
		and pipeline_monthly.stage_name_this_month
			= default_probabilities.stage_name
