with
	opportunities_direct as (
		select *
		from {{ ref('salesforce_opportunities_detailed_direct' )}}
	)

	, stalled_period as (
		select *
		from {{ ref('salesforce_opportunities_stalled_period') }}
	)

select opportunities_direct.*
	, case
		when opportunities_direct.decide_date < current_date then 'Decide'
		when opportunities_direct.justify_date < current_date then 'Justify'
		when opportunities_direct.validate_date < current_date then 'Validate'
		when opportunities_direct.educate_date < current_date then 'Educate'
		when opportunities_direct.initiate_date < current_date then 'Initiate'
		else 'Meeting Booked'
	end as stage_name_this_month
	, date_trunc('month', opportunities_direct.created_date) + interval '1 month'
		= date_trunc('month', current_date) as is_first_month_in_pipeline
	, date_trunc('month', opportunities_direct.close_date)
		= date_trunc('month', current_date) as is_last_month_in_pipeline
from opportunities_direct
left join stalled_period
	on current_timestamp <@ stalled_period.stalled_range
		and opportunities_direct.opportunity_id = stalled_period.opportunity_id
where stalled_period.opportunity_id is null
	and opportunities_direct.created_date <= current_date
	and current_date < opportunities_direct.close_date
	and opportunities_direct.close_date
		< current_date +
			case
				when owner_title = 'Enterprise Account Executive'
				then interval '9 months'
				else interval '3 months'
			end
