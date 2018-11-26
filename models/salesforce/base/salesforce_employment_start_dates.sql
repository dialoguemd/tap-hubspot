select user_id
	, start_date::timestamp
from {{ ref('data_salesforce_employment_start_dates') }}
