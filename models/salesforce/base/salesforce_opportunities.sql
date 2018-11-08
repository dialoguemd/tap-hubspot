select id as opportunity_id
	, name as opportunity_name
	, partner_influence_c as partner_influence
	, partner_type_c as partner_type
	, lead_source
	, coalesce(revenue_type_c, 'N/A') as revenue_type
	, stage_name
	, close_date
	, is_closed
	, is_won
	, account_id
	, owner_id
	, billing_start_date_c as billing_start_date
	, i_date_c as i_date
	, coalesce(billing_start_date_c, i_date_c) as launch_date
	, coalesce(self_signup_no_touch_c, false) as self_signup_no_touch
	, number_of_employees_c as number_of_employees
	, amount::float as amount
	, created_date
	, extract('day' from current_date - coalesce(
	    initiate_date_c, meeting_date_c,
	    educate_date_c, validate_date_c,
	    justify_date_c, decide_date_c,
	    case when is_closed and is_won
	      then close_date
	      else null
	    end)) as opportunity_age
	-- backfill meeting date with the initiate date if one exists
	, coalesce(initiate_date_c, meeting_date_c,
	    educate_date_c, validate_date_c,
	    justify_date_c, decide_date_c,
	    case when is_closed and is_won
	      then close_date
	      else null
	    end) as meeting_date
	, coalesce(initiate_date_c, educate_date_c,
	      validate_date_c, justify_date_c,
	      decide_date_c,
	      case
	        when is_closed and is_won
	        then close_date
	        else null
	      end
	    ) as initiate_date
	, coalesce(educate_date_c, validate_date_c,
	      justify_date_c, decide_date_c,
	      case
	        when is_closed and is_won
	        then close_date
	        else null
	      end
	    ) as educate_date
	, coalesce(validate_date_c, justify_date_c,
	      decide_date_c,
	      case
	        when is_closed and is_won
	        then close_date
	        else null
	      end
	    ) as validate_date
	, coalesce(justify_date_c, decide_date_c,
	    case
	        when is_closed and is_won
	        then close_date
	        else null
	      end
	    ) as justify_date
	, coalesce(decide_date_c,
	      case
	        when is_closed and is_won
	        then close_date
	        else null
	      end
	    ) as decide_date
	, case
	    when stage_name = 'Closed Won'
	    then close_date
	    else null
	  end as close_won_date
	, case
	    when stage_name = 'Closed Won'
	    then extract('day' from close_date - coalesce(
	        meeting_date_c, initiate_date_c,
	        educate_date_c, validate_date_c,
	        justify_date_c, decide_date_c,
	        case when is_closed and is_won
	          then close_date
	          else null
	        end))
	      else null
	    end as time_to_close_won
	, case when number_of_employees_c is null
		then
			case
				when amount is null then 'N/A'
				when amount::float < 400 then '1-39'
				when amount::float < 900 then '40-99'
				when amount::float < 1800 then '100-199'
				when amount::float < 4500 then '200-499'
				when amount::float < 8000 then '500-999'
			else '1000+'
			end
		when number_of_employees_c < 40 then '1-39'
		when number_of_employees_c < 100 then '40-99'
		when number_of_employees_c < 200 then '100-199'
		when number_of_employees_c < 500 then '200-499'
		when number_of_employees_c < 1000 then '500-999'
		else '1000+'
	end as segment
	, probability::float / 100 as probability
	, case
		when value_period_c is null then 'Monthly'
		when value_period_c = 'one_time' then 'One Time'
		else value_period_c
	end as value_period
	, pilot_c is not null and pilot_c = 'Yes' as is_pilot
from salesforce.opportunities
where not is_deleted
