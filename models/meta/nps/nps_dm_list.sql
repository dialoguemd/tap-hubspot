with
    organizations as (
        select *
        from {{ ref('scribe_organizations_active_today') }}
        where is_paid
    )

    , salesforce_scribe_organizations as (
        select * from {{ ref('salesforce_scribe_organizations_detailed') }}
    )

    , users as (
        select * from {{ ref('scribe_users') }}
    )

    , nps_survey_dm as (
        select * from {{ ref('delighted_survey_decision_maker') }}
    )

    , sf_contacts as (
        select * from {{ ref('salesforce_contacts') }}
    )

    , contacts as (
        select sf_contacts.email
            , sf_contacts.first_name
            , sf_contacts.contact_type
            , coalesce(
                lower(users.language)
                , case
                    when organizations.email_preference
                        in ('english', 'bilingual-english-french')
                    then 'en'
                    when organizations.email_preference
                        in ('french', 'bilingual-french-english')
                    then 'fr'
                    else organizations.email_preference
                 end) as language
            , organizations.billing_start_date
            , organizations.organization_id
            , row_number()
                over(
                    partition by sf_contacts.email
                    order by organizations.billing_start_date
                ) as rank
        from salesforce_scribe_organizations
        inner join organizations
            using (organization_id)
        inner join sf_contacts
            using (account_id)
        left join users
            using (email)
        where contact_type in ('HR + DM (do not use - old)',
            'Decision-Maker', 'HR Contact (User)', 'C-suite Executive',
            'HR Primary Contact', 'Account support', 'Decision Maker')
            and sf_contacts.email is not null
    )

select contacts.email
    , contacts.contact_type
    , contacts.organization_id
    , trunc(
        extract(
            day from
            current_timestamp - billing_start_date) / 30
        )::int
        as month_since_billing_start_date
    , case when contacts.language = 'en'
        then 'en-dialogue-2'
        else 'fr-dialogue-2'
    end as locale
    , case when contacts.language = 'en' and contacts.first_name <> ''
        then contacts.first_name ||
', how likely are you to recommend Dialogue?'
        when contacts.language = 'en'
        then 'How likely are you to recommend Dialogue?'
        when contacts.first_name <> ''
        then contacts.first_name ||
', seriez-vous prêt(e) à recommander Dialogue?'
        else 'Seriez-vous prêt(e) à recommander Dialogue?'
    end as delighted_email_subject
    , case when contacts.language = 'en' and contacts.first_name <> ''
        then contacts.first_name ||
', we would love your feedback! This survey ' ||
'will take less than a minute, and will help us improve our services'
        when contacts.language = 'en'
        then 'We would love your feedback! This survey ' ||
'will take less than a minute, and will help us improve our services'
        when contacts.first_name <> ''
        then contacts.first_name ||
', nous aimerions recevoir vos commentaires! ' ||
'Ce questionnaire prendra moins d''une minute,' ||
' et nous aidera à améliorer nos services'
        else 'Nous aimerions recevoir vos commentaires! ' ||
'Ce questionnaire prendra moins d''une minute,' ||
' et nous aidera à améliorer nos services'
    end as delighted_intro_message
    , current_timestamp as timestamp
from contacts
left join nps_survey_dm
    on contacts.email = nps_survey_dm.email
    -- email not sent if the user replied in the last 2 months
        and nps_survey_dm.timestamp > current_timestamp - interval '60 days'
where contacts.rank = 1
    -- email sent 5 and 10 months after onboarding
    -- if the user didn't respond to the first batch,
    --   we send it again after 1 month (ie: month 2, 6, 11)
    and trunc(
            extract(
                day from
                current_timestamp - contacts.billing_start_date
            ) / 30
        )::int
        in (2, 3, 5, 6, 10, 11)
    and nps_survey_dm.email is null
