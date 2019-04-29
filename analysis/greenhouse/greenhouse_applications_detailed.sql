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
                WHEN 'Psychology Today' THEN 'Sourced'
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
                WHEN 'Facebook' THEN 'Sourced'
                WHEN 'Campus recruiting' THEN 'Sourced'
                WHEN 'Collage' THEN 'Applied'
                WHEN 'Sourcinc' THEN 'Agencies'
                WHEN 'Twitter' THEN 'Sourced'
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
            , bool_or(
                scheduled_interviews.stage_name in
                ('Case Study', 'Technical Interview', 'Technical interview')
            ) as has_case_study
            , bool_or(
                scheduled_interviews.stage_name in
                ('Topgrading')
            ) as has_topgrading
            , bool_or(
                scheduled_interviews.stage_name in
                ('Reference Check', 'Reference check')
            ) as has_reference_check
        from scheduled_interviews
        group by 1
    )

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
