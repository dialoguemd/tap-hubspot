with reply as (
    select * from {{ ref('countdown_question_replied') }}
    )

    , episodes_chief_complaint as (
    select * from {{ ref('episodes_chief_complaint') }}
    )

    , dxa_cc as (
        select * from {{ ref('dimension_dxa_chief_complaints') }}
    )

    , doctor_validated as (
        select * from {{ ref('data_dxa_cc_classifier_data_validated') }}
    )

select reply.episode_id
    , min(reply.reply_value) as descript
    , min(reply.replied_at) as replied_at
    , case
      	when min(dxa_cc.chief_complaint) is null then null
      	else min(episodes_chief_complaint.cc_code)
      	end as shift_manager_label
from reply
inner join episodes_chief_complaint
    using (episode_id)
left join dxa_cc
	on episodes_chief_complaint.cc_code = dxa_cc.chief_complaint
left join doctor_validated
	on reply.reply_value = doctor_validated.descript
where reply.question_name = 'symptoms' 
    and reply.reply_value is not null
    and qnaire_name in ('feeling_sick', 'chronic', 'ask_symptoms')
    and doctor_validated.doctor_label is null
group by 1
having count(*) = 1