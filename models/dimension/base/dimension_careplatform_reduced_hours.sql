select date_day::timestamp
from {{ ref('data_dimension_careplatform_reduced_hours')}}
