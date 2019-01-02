with ccs as (
        select * from {{ ref('messaging_dxa_cc') }}
    )

    , cc_labels as (
        select * from {{ ref('messaging_dxa_labels') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , joined as (
        select episodes.episode_id
            , ccs.cc
            , cc_labels.label
            , episodes.issue_type
            , episodes.outcome
        from episodes
        inner join ccs
            using (episode_id)
        inner join cc_labels
            using (cc)
    )

select *
    , 'left' as viz_side
from joined
union all
select *
    , 'right' as viz_side
from joined
