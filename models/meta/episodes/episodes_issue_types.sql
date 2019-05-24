with careplatform_episode_properties_updated as (
        select * from {{ ref('careplatform_episode_properties_updated') }}
    )

    , issue_type_rank as (
        select episode_id
            , episode_property_value as issue_type
            , updated_at
            , row_number() over(partition by episode_id order by updated_at desc) as rank
        from careplatform_episode_properties_updated
        where episode_property_type = 'issue_type'
            and episode_id is not null
    )

    , episode_issue_type as (
        select episode_id
            , case
                when issue_type in ('adhdp', 'biomp', 'cardiop', 'dermp'
                    , 'dietp', 'endop', 'entp', 'gip', 'gup', 'gynp', 'mskp', 'navp'
                    , 'neurop', 'obsp', 'ophtp', 'otherp', 'psyp', 'respp'
                    , 'rxp', 'sleepp', 'travelp')
                then regexp_replace(issue_type, 'p$', '')
                else issue_type
                end as issue_type
            , issue_type in ('adhdp', 'biomp', 'cardiop', 'dermp'
                    , 'dietp', 'endop', 'entp', 'gip', 'gup', 'gynp', 'mskp', 'navp'
                    , 'neurop', 'obsp', 'ophtp', 'otherp', 'psyp', 'respp'
                    , 'rxp', 'sleepp', 'travelp') as is_issue_pediatrics
            , updated_at
        from issue_type_rank
        where rank = 1
    )

select episode_id
    , issue_type
    , is_issue_pediatrics
    , updated_at as issue_type_set_timestamp
from episode_issue_type
