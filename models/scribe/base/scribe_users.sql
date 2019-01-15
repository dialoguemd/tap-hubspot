select id as user_id
    , address_country
    , address_state
    , age
    , case when birthday is null
        then created_at - interval '1 year' * age
        else birthday
    end as birthday
    , country
    , created_at
    , email
    , first_name
    , last_name
    , first_name || ' ' || last_name as user_name
    , coalesce(gender, 'N/A') as gender
    , coalesce(upper(language), 'N/A') as language
    , case
        when residence_province = 'QC' then 'Quebec'
        when residence_province = 'ON' then 'Ontario'
        when residence_province = 'MB' then 'Manitoba'
        when residence_province = 'YT' then 'Yukon'
        when residence_province = 'AB' then 'Alberta'
        when residence_province = 'SK' then 'Saskatchewan'
        when residence_province = 'BC' then 'British Columbia'
        when residence_province = 'NB' then 'New Brunswick'
        when residence_province = 'NS' then 'Nova Scotia'
        when residence_province = 'PE' then 'Prince Edward Island'
        when residence_province = 'NL' then 'Newfoundland and Labrador'
        else residence_province
    end as residence_province
    , auth_id
    , coalesce(status, 'invited') as status
    , family_id
    , received_at
from scribe.users
