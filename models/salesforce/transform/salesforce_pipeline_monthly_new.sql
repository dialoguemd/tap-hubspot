with
	opportunities as (
		select *
		from {{ ref('salesforce_opportunities_detailed_direct') }}
	)

	, probabilities as (
		select *
		from {{ ref('salesforce_pipeline_probabilities')}}
	)

select opportunities.*
	, probabilities.probability as initial_probability
from opportunities
inner join probabilities
	on opportunities.segment = probabilities.segment
		and probabilities.stage_name = 'Meeting Booked'
