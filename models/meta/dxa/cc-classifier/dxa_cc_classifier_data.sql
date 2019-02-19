with reply as (
        select * from {{ ref('countdown_question_replied') }}
    )

    , episodes_chief_complaint as (
        select * from {{ ref('episodes_chief_complaint') }}
    )

    , doctor_validated as (
        select * from {{ ref('data_dxa_cc_classifier_data_validated') }}
    )

    , users as (
    	select * from {{ ref('scribe_users') }}
    )

    , ccs_parsed_tmp as (
        select * from {{ ref('countdown_chief_complaint_parsed') }}
    )

    , ccs_parsed as (
        select episode_id
            , cc_name
            , cc_confidence
            , row_number()
                over (partition by episode_id order by cc_confidence desc)
                as rank
        from ccs_parsed_tmp
    )

select reply.episode_id
    , episodes_chief_complaint.cc_code as shift_manager_label
    , min(doctor_validated.doctor_label) as doctor_label
    , min(reply.reply_value) as descript
    , min(reply.replied_at) as replied_at
    , min(users.language) as lang

    -- Jinja loop for top three CCs
    {% for rank in ['1', '2', '3'] %}

    , min(ccs_parsed.cc_name) filter (where rank = {{rank}})
        as cc_name_rank_{{rank}}

    , min(ccs_parsed.cc_confidence) filter (where rank = {{rank}})
        as cc_confidence_rank_{{rank}}

    {% endfor %}

from reply
left join episodes_chief_complaint
    using (episode_id)
left join ccs_parsed
    using (episode_id)
left join doctor_validated
	on reply.reply_value = doctor_validated.descript
left join users
	using (user_id)
where reply.question_name = 'symptoms'
    and reply.reply_value is not null
    and qnaire_name in ('feeling_sick', 'chronic', 'ask_symptoms')
group by 1, 2
having count(*) = 1
