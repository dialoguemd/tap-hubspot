with validated_data as (select * from {{ ref('data_dxa_cc_classifier_data_validated') }}
)

, ccs as (select * from {{ ref('dimension_dxa_chief_complaints') }}
)

select * from validated_data
left join ccs
on validated_data.doctor_label = ccs.chief_complaint