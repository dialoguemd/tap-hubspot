with reply as (
        select * from {{ ref('countdown_question_replied') }}
    )

    , episodes_chief_complaint as (
        select * from {{ ref('episodes_chief_complaint') }}
    )

    , doctor_validated as (
        select * from {{ ref('data_dxa_cc_classifier_data_validated') }}
    )

select reply.episode_id
    , episodes_chief_complaint.cc_code as shift_manager_label
    , min(reply.reply_value) as descript
    , min(reply.replied_at) as replied_at
from reply
left join episodes_chief_complaint
    using (episode_id)
left join doctor_validated
	on reply.reply_value = doctor_validated.descript
where reply.question_name = 'symptoms' 
    and reply.reply_value is not null
    and qnaire_name in ('feeling_sick', 'chronic', 'ask_symptoms')
    and doctor_validated.doctor_label is null
group by 1, 2
having count(*) = 1