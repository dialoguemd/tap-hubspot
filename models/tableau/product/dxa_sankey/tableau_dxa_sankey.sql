# This doesn't need to run on a schedule anymore
{{
  config(
    enabled=False
  )
}}

with ccs as (
        select * from {{ ref('dxa_launched_cc') }}
    )

    , cc_labels as (
        select * from {{ ref('dimension_dxa_chief_complaints') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , joined as (
        select episodes.episode_id
            , ccs.cc
            , cc_labels.description_en as label
            , episodes.issue_type
            , episodes.outcome
        from episodes
        inner join ccs
            using (episode_id)
        inner join cc_labels
            on ccs.cc = cc_labels.chief_complaint
    )

select *
    , 'left' as viz_side
from joined
union all
select *
    , 'right' as viz_side
from joined
