select date_month
    , title
    , province
    , count(*) as user_count
from {{ ref('salesforce_users_month')}}
group by 1,2,3
