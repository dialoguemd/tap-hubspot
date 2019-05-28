-- This points at the Greenhouse Amazon Redshift BI Connector

with
    sources_cte as (
        select id as source_id
            , name as source_name
            , case 
                WHEN name like 'Applied%' THEN 'Applied'
                WHEN name like 'Sourced%' THEN 'Sourced'
                WHEN name = 'Applied by email' THEN 'Applied'
                WHEN name = 'Other' THEN 'Other'
                WHEN name = 'Jobs page on your website' THEN 'Applied'
                WHEN name = 'Applied on Indeed' THEN 'Applied'
                WHEN name = 'Referral' THEN 'Referral'
                WHEN name = 'Indeed' THEN 'Applied'
                WHEN name = 'LinkedIn (Prospecting)' THEN 'Sourced'
                WHEN name = 'LinkedIn Job Posting' THEN 'Applied'
                WHEN name = 'Entelo' THEN 'Sourced'
                WHEN name = 'Leadership Agency' THEN 'Agencies'
                WHEN name = 'LinkedIn (Connection)' THEN 'Sourced'
                WHEN name = 'Psychology Today' THEN 'Sourced'
                WHEN name = 'Internal Applicant' THEN 'Applied'
                WHEN name = 'OIIQ Bank' THEN 'Applied'
                WHEN name = 'AngelList' THEN 'Applied'
                WHEN name = 'ISARTA' THEN 'Applied'
                WHEN name = 'The Leadership Agency' THEN 'Agencies'
                WHEN name = 'Google' THEN 'Applied'
                WHEN name = 'Email' THEN 'Applied'
                WHEN name = 'LinkedIn (Social Media)' THEN 'Applied'
                WHEN name = 'Ordre des psychologues' THEN 'Applied'
                WHEN name = 'Customer newsletter' THEN 'Applied'
                WHEN name = 'Facebook' THEN 'Sourced'
                WHEN name = 'Campus recruiting' THEN 'Sourced'
                WHEN name = 'Collage' THEN 'Applied'
                WHEN name = 'Sourcinc' THEN 'Agencies'
                WHEN name = 'Twitter' THEN 'Sourced'
                WHEN name = 'Networking' THEN 'Sourced'
                WHEN name = 'Job fairs/Conferences/Trade shows' THEN 'Sourced'
                WHEN name = 'AEE Placement' THEN 'Sourced'
                WHEN name = 'Meetups' THEN 'Sourced'
                WHEN name = 'Applied on website' THEN 'Applied'
                WHEN name = 'Applied by Linkedin message' THEN 'Applied'
                ELSE 'Other'
                END as source_grouping_name
        from sources
    )

    , rejection_reasons_cte as (
        select id as rejection_reason_id
            , name as rejection_reason
            , case name
                WHEN 'Underqualified' THEN 'Underqualified'
                WHEN 'Skills Mismatch' THEN 'Underqualified'
                WHEN 'Profile Mismatch' THEN 'Underqualified'
                WHEN 'Language' THEN 'Underqualified'
                WHEN 'Culture Mismatch' THEN 'Underqualified'
                WHEN 'Overqualified' THEN 'Overqualified'
                WHEN 'Looking for a more senior role' THEN 'Overqualified'
                WHEN 'Compensation Expectations' THEN 'Compensation Expectations'
                WHEN 'Timing' THEN 'Timing'
                WHEN 'Looking for part-time' THEN 'Timing'
                WHEN 'Preferred Another Candidate' THEN 'Timing'
                WHEN 'Preferred another candidate' THEN 'Timing'
                WHEN 'Position Closed' THEN 'Timing'
                WHEN 'Happy where they are' THEN 'Timing'
                WHEN 'Looking for Contract Work' THEN 'Timing'
                WHEN 'Mat leave' THEN 'Timing'
                WHEN 'Parental Leave' THEN 'Timing'
                WHEN 'Traveling' THEN 'Timing'
                WHEN 'No Sourcing List' THEN 'No Sourcing List'
                WHEN 'Works for client' THEN 'No Sourcing List'
                WHEN 'Works for a friend company' THEN 'No Sourcing List'
                WHEN 'Visa Status' THEN 'Other'
                WHEN 'Location' THEN 'Other'
                WHEN 'Duplicate' THEN 'Other'
                WHEN 'Personal reasons' THEN 'Other'
                WHEN 'Chose another opportunity' THEN 'Offer Declined'
                WHEN 'Offer Declined' THEN 'Offer Declined'
                WHEN 'Not interested' THEN 'Not Interested'
                WHEN 'Unresponsive' THEN 'Not Interested'
                WHEN 'Not interested in tech stack' THEN 'Not Interested'
                WHEN 'Withdrew' THEN 'Not Interested'
                ELSE 'Other'
                END as rejection_reason_grouping
        from rejection_reasons
    )

    , interviews_cte as (
        select application_id
            , count(scheduled_interviews.application_id) as interviews_count
            , bool_or(
                scheduled_interviews.stage_name in
                ('Hiring Manager Screening', 'Hiring manager screen', 'Pitch', 'Screening')
            ) as has_screening
            , min(
                case when scheduled_interviews.stage_name in
                    ('Hiring Manager Screening', 'Hiring manager screen', 'Pitch', 'Screening')
                then scheduled_at
                else null
                end
            ) as screening_at
            , bool_or(
                scheduled_interviews.stage_name in
                ('Case Study', 'Technical Interview', 'Technical interview')
            ) as has_case_study
            , min(
                case when scheduled_interviews.stage_name in
                    ('Case Study', 'Technical Interview', 'Technical interview')
                then scheduled_at
                else null
                end
            ) as case_study_at
            , bool_or(
                scheduled_interviews.stage_name in
                ('Topgrading')
            ) as has_topgrading
            , min(
                case when scheduled_interviews.stage_name in
                    ('Topgrading')
                then scheduled_at
                else null
                end
            ) as topgrading_at
            , bool_or(
                scheduled_interviews.stage_name in
                ('Reference Check', 'Reference check')
            ) as has_reference_check
            , min(
                case when scheduled_interviews.stage_name in
                    ('Reference Check', 'Reference check')
                then scheduled_at
                else null
                end
            ) as reference_check_at
        from scheduled_interviews
        group by 1
    )

    , joined as (
        select applications.candidate_id
            , applications.applied_at
            , coalesce(
                applications.rejected_at,
                offers.resolved_at) as application_closed_at
            , applications.status as application_status
            , applications.prospect as application_prospect
            , users.first_name || ' ' || users.last_name as recruiter
            , sources_cte.source_name
            , coalesce(
                sources_cte.source_grouping_name
                , 'Other'
                ) as source_grouping_name
            , jobs.status as job_status
            , jobs.opened_at as job_opened_at
            , jobs.closed_at as job_closed_at
            , jobs.name as job_name
            , departments.name as department_name
            , rejection_reasons_cte.rejection_reason
            , rejection_reasons_cte.rejection_reason_grouping
            , offices.name as office_name
            , offers.sent_at as offer_sent_at
            , offers.resolved_at as offer_resolved_at
            , coalesce(
                offers.status,
                'no_offer'
                ) as offer_status
            , interviews_cte.interviews_count
            , coalesce(interviews_cte.has_screening
                OR interviews_cte.has_case_study
                OR interviews_cte.has_topgrading
                OR interviews_cte.has_reference_check
                OR applications.status = 'hired', False) as has_screening
            , coalesce(interviews_cte.has_case_study
                OR interviews_cte.has_topgrading
                OR interviews_cte.has_reference_check
                OR applications.status = 'hired', False) as has_case_study
            , coalesce(interviews_cte.has_topgrading
                OR interviews_cte.has_reference_check
                OR applications.status = 'hired', False) as has_topgrading
            , coalesce(interviews_cte.has_reference_check
                OR applications.status = 'hired', False) as has_reference_check
            , case
                when applications.status = 'hired' then 'hired'
                when applications.status = 'rejected' then 'rejected'
                when interviews_cte.has_reference_check then 'reference_check'
                when interviews_cte.has_topgrading then 'topgrading'
                when interviews_cte.has_case_study then 'case_study'
                when interviews_cte.has_screening then 'screening'
                else 'new'
                end as application_stage
            , screening_at
            , case_study_at
            , topgrading_at
            , reference_check_at
        from applications
        left join sources_cte
            using (source_id)
        left join applications_jobs
            on applications.id = applications_jobs.application_id
        left join jobs
            on applications_jobs.job_id = jobs.id
        left join departments
            on jobs.department_id = departments.id
        left join interviews_cte
            on applications.id = interviews_cte.application_id
        left join jobs_offices
            on jobs.id = jobs_offices.job_id
        left join offices
            on jobs_offices.office_id = offices.id
        left join offers
            on applications.id = offers.application_id
        left join rejection_reasons_cte
            using (rejection_reason_id)
        left join candidates
            on applications.candidate_id = candidates.id
        left join users
            on candidates.recruiter_id = users.id
    )

select *
    , case
        when application_stage = 'new' and department_name in ('Tech', 'Product') then 0
        when application_stage = 'new' and department_name = 'Medical Operations' then 0
        when application_stage = 'new' and department_name = 'Sales' then 0
        when application_stage = 'new' then 0
        when application_stage = 'screening' and department_name in ('Tech', 'Product') then 0.108695652173913
        when application_stage = 'screening' and department_name = 'Medical Operations' then 0.226190476190476
        when application_stage = 'screening' and department_name = 'Sales' then 0.0443548387096774
        when application_stage = 'screening' then 0.0892857142857143
        when application_stage = 'case_study' and department_name in ('Tech', 'Product') then 0.283018867924528
        when application_stage = 'case_study' and department_name = 'Medical Operations' then 0.539007092198582
        when application_stage = 'case_study' and department_name = 'Sales' then 0.20952380952381
        when application_stage = 'case_study' then 0.357142857142857
        when application_stage = 'topgrading' and department_name in ('Tech', 'Product') then 0.535714285714286
        when application_stage = 'topgrading' and department_name = 'Medical Operations' then 0.554744525547445
        when application_stage = 'topgrading' and department_name = 'Sales' then 0.372881355932203
        when application_stage = 'topgrading' then 0.476190476190476
        when application_stage = 'reference_check' and department_name in ('Tech', 'Product') then 0.9375
        when application_stage = 'reference_check' and department_name = 'Medical Operations' then 1
        when application_stage = 'reference_check' and department_name = 'Sales' then 0.88
        when application_stage = 'reference_check' then 0.909090909090909
        when application_stage = 'hired' and department_name in ('Tech', 'Product') then 1
        when application_stage = 'hired' and department_name = 'Medical Operations' then 1
        when application_stage = 'hired' and department_name = 'Sales' then 1
        when application_stage = 'hired' then 1
        when application_stage = 'rejected' and department_name in ('Tech', 'Product') then 0
        when application_stage = 'rejected' and department_name = 'Medical Operations' then 0
        when application_stage = 'rejected' and department_name = 'Sales' then 0
        when application_stage = 'rejected' then 0
        else 0
        end as hire_likehlihood
from joined
