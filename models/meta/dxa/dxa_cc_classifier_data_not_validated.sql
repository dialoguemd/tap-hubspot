with data as (
        select * from {{ ref('dxa_cc_classifier_data') }}
    )

select *
from data
where doctor_label is null
