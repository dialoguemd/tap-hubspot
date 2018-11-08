with episodes_chats_all_time as (
        select * from {{ ref('pdt_chats_all_time') }}
    )

    , cp_activity as (
        select * from {{ ref('pdt_cp_activity') }}
    )

    , ttr as (
        select episode_id
            , extract(epoch from (max(last_message_created_at) - min(first_message_created_at))) as ttr
        from episodes_chats_all_time
        group by 1
    )

    , episode_activity as (
        select episode_id
            , first_message_created_at
            , coalesce(sum(time_spent), 0) as attr_total
            , coalesce(sum(time_spent) filter(where main_specialization = 'Nurse Clinician'), 0) as attr_nc
            , coalesce(sum(time_spent) filter(where main_specialization = 'Nurse Practitioner'), 0) as attr_np
            , coalesce(sum(time_spent) filter(where main_specialization in ('Care Coordinator', 'Medical Assistant')), 0) as attr_cc
            , coalesce(sum(time_spent) filter(where main_specialization = 'Family Physician'), 0) as attr_gp
            , coalesce(sum(time_spent) filter(where main_specialization = 'Psychologist'), 0) as attr_psy
            , coalesce(sum(time_spent) filter(where main_specialization = 'Nutritionist'), 0) as attr_nutr

            , coalesce(sum(time_spent) filter(where cp_activity.date = chats_all_time.created_at_day), 0) as attr_total_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nurse Clinician'
                    and cp_activity.date = chats_all_time.created_at_day
                ), 0) as attr_nc_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nurse Practitioner'
                    and cp_activity.date = chats_all_time.created_at_day
                ), 0) as attr_np_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization in ('Care Coordinator', 'Medical Assistant')
                    and cp_activity.date = chats_all_time.created_at_day
                    ), 0) as attr_cc_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Family Physician'
                    and cp_activity.date = chats_all_time.created_at_day
                    ), 0) as attr_gp_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Psychologist'
                    and cp_activity.date = chats_all_time.created_at_day
                    ), 0) as attr_psy_day_1
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nutritionist'
                    and cp_activity.date = chats_all_time.created_at_day
                    ), 0) as attr_nutr_day_1

            , coalesce(sum(time_spent) filter(where cp_activity.date < chats_all_time.created_at_day + interval '7 days'), 0) as attr_total_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nurse Clinician'
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                ), 0) as attr_nc_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nurse Practitioner'
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                ), 0) as attr_np_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization in ('Care Coordinator', 'Medical Assistant')
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                    ), 0) as attr_cc_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Family Physician'
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                    ), 0) as attr_gp_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Psychologist'
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                    ), 0) as attr_psy_day_7
            , coalesce(sum(time_spent) filter(where
                    main_specialization = 'Nutritionist'
                    and cp_activity.date < chats_all_time.created_at_day + interval '7 days'
                    ), 0) as attr_nutr_day_7
        from episodes_chats_all_time as chats_all_time
        inner join cp_activity
            using (episode_id)
        where chats_all_time.chat_type = 'New Episode'
            and cp_activity.is_active
        group by 1,2
    )

    select episode_activity.episode_id
        , ttr / 60.0 as ttr_total
        , attr_total / 60.0 as attr_total
        , attr_nc / 60.0 as attr_nc
        , attr_np / 60.0 as attr_np
        , attr_nc / 60.0
            + attr_np / 60.0 as attr_nurse
        , attr_cc / 60.0 as attr_cc
        , attr_gp / 60.0 as attr_gp
        , attr_psy / 60.0 as attr_psy
        , attr_nutr / 60.0 as attr_nutr
        , attr_total_day_1 / 60.0 as attr_total_day_1
        , attr_nc_day_1 / 60.0 as attr_nc_day_1
        , attr_np_day_1 / 60.0 as attr_np_day_1
        , attr_nc_day_1 / 60.0
            + attr_np_day_1 / 60.0 as attr_nurse_day_1
        , attr_cc_day_1 / 60.0 as attr_cc_day_1
        , attr_gp_day_1 / 60.0 as attr_gp_day_1
        , attr_psy_day_1 / 60.0 as attr_psy_day_1
        , attr_nutr_day_1 / 60.0 as attr_nutr_day_1
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_total_day_7 / 60.0
            end as attr_total_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_nc_day_7 / 60.0
            end as attr_nc_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_np_day_7 / 60.0
            end as attr_np_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_nc_day_7 / 60.0
         + attr_np_day_7 / 60.0
            end as attr_nurse_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_cc_day_7 / 60.0
            end as attr_cc_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_gp_day_7 / 60.0
            end as attr_gp_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_psy_day_7 / 60.0
            end as attr_psy_day_7
        , case
                when first_message_created_at > current_date - interval '7 days'
                then null
                else attr_nutr_day_7 / 60.0
            end as attr_nutr_day_7
    from episode_activity
    left join ttr
        on episode_activity.episode_id = ttr.episode_id
