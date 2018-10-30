select *
	, generate_series(
        date_trunc('month', started_date),
        date_trunc('month', last_login_date),
        interval '1 month'
      ) as date_month
from {{ ref('salesforce_users')}}
