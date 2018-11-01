with
	opportunities_direct as (
		select * from {{ ref('salesforce_opportunities_detailed_direct' )}}
	)

	, dim_months as (
		select * from {{ ref('dimension_months')}}
	)

	, stalled_period as (
		select *
		from {{ ref('salesforce_opportunities_stalled_period') }}
	)

select dim_months.date_month
	, opportunities_direct.*
	, case
		when opportunities_direct.decide_date < dim_months.date_month then 'Decide'
		when opportunities_direct.justify_date < dim_months.date_month then 'Justify'
		when opportunities_direct.validate_date < dim_months.date_month then 'Validate'
		when opportunities_direct.educate_date < dim_months.date_month then 'Educate'
		when opportunities_direct.initiate_date < dim_months.date_month then 'Initiate'
		else 'Meeting Booked'
	end as stage_name_this_month
	, date_trunc('month', opportunities_direct.created_date) + interval '1 month'
		= dim_months.date_month as is_first_month_in_pipeline
	, date_trunc('month', opportunities_direct.close_date)
		= dim_months.date_month as is_last_month_in_pipeline
	, case
		when date_trunc('month', opportunities_direct.created_date) + interval '1 month'
			= dim_months.date_month
		then 'New'
		when date_trunc('month', opportunities_direct.close_date)
			= dim_months.date_month and opportunities_direct.is_won
		then 'Closed Won'
		when date_trunc('month', opportunities_direct.close_date)
		= dim_months.date_month and not opportunities_direct.is_won
		then 'Closed Lost'
		else 'Other'
	end as mom_evolution
from dim_months
inner join opportunities_direct
	on dim_months.date_month >= opportunities_direct.created_date
		and dim_months.date_month < opportunities_direct.close_date
left join stalled_period
	on dim_months.date_month <@ stalled_period.stalled_range
		and opportunities_direct.opportunity_id = stalled_period.opportunity_id
where stalled_period.opportunity_id is null
