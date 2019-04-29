-- This points at the Greenhouse Amazon Redshift BI Connector

with
    sources_cte as (
        select id as source_id
            , name as source_name
            , case name 
                WHEN 'Applied by email' THEN 'Applied'
                WHEN 'Other' THEN 'Other'
                WHEN 'Jobs page on your website' THEN 'Applied'
                WHEN 'Applied on Indeed' THEN 'Applied'
                WHEN 'Referral' THEN 'Referral'
                WHEN 'Indeed' THEN 'Applied'
                WHEN 'LinkedIn (Prospecting)' THEN 'Sourced'
                WHEN 'LinkedIn Job Posting' THEN 'Applied'
                WHEN 'Entelo' THEN 'Sourced'
                WHEN 'Leadership Agency' THEN 'Agencies'
                WHEN 'LinkedIn (Connection)' THEN 'Sourced'
                WHEN 'Psychology Today' THEN 'Applied'
                WHEN 'Internal Applicant' THEN 'Applied'
                WHEN 'OIIQ Bank' THEN 'Applied'
                WHEN 'AngelList' THEN 'Applied'
                WHEN 'ISARTA' THEN 'Applied'
                WHEN 'The Leadership Agency' THEN 'Agencies'
                WHEN 'Google' THEN 'Applied'
                WHEN 'Email' THEN 'Applied'
                WHEN 'LinkedIn (Social Media)' THEN 'Applied'
                WHEN 'Ordre des psychologues' THEN 'Applied'
                WHEN 'Customer newsletter' THEN 'Applied'
                WHEN 'Facebook' THEN 'Applied'
                WHEN 'Campus recruiting' THEN 'Sourced'
                WHEN 'Collage' THEN 'Sourced'
                WHEN 'Sourcinc' THEN 'Agencies'
                WHEN 'Twitter' THEN 'Applied'
                WHEN 'Networking' THEN 'Sourced'
                WHEN 'Job fairs/Conferences/Trade shows' THEN 'Sourced'
                WHEN 'AEE Placement' THEN 'Sourced'
                WHEN 'Meetups' THEN 'Sourced'
                WHEN 'Applied on website' THEN 'Applied'
                WHEN 'Applied by Linkedin message' THEN 'Applied'
                ELSE 'Other'
                END as source_grouping_name
        from sources
    )

select applications.candidate_id
    , applications.applied_at
    , applications.status as application_status
    , applications.prospect as application_prospect
    , users.first_name || ' ' || users.last_name as recruiter
    , sources_cte.source_name
    , sources_cte.source_grouping_name
    , jobs.status as job_status
    , jobs.opened_at as job_opened_at
    , jobs.closed_at as job_closed_at
    , jobs.name as job_name
    , departments.name as department_name
    , rejection_reasons.name as rejection_reason
    , offices.name as office_name
    , offers.sent_at as offer_sent_at
    , offers.resolved_at as offer_resolved_at
    , coalesce(
        offers.status,
        'no_offer'
        ) as offer_status
    , scheduled_interviews.stage_name as interview_stage_name
    , scheduled_interviews.interview_name
    , scheduled_interviews.scheduled_at
from applications
left join sources_cte
    using (source_id)
left join applications_jobs
    on applications.id = applications_jobs.application_id
left join jobs
    on applications_jobs.job_id = jobs.id
left join departments
    on jobs.department_id = departments.id
left join scheduled_interviews
    on applications.id = scheduled_interviews.application_id
left join jobs_offices
    on jobs.id = jobs_offices.job_id
left join offices
    on jobs_offices.office_id = offices.id
left join offers
    on applications.id = offers.application_id
left join rejection_reasons
    on applications.rejection_reason_id = rejection_reasons.id
left join candidates
    on applications.candidate_id = candidates.id
left join users
    on candidates.recruiter_id = users.id
