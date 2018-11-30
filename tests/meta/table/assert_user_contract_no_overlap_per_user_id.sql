with user_contracts as (
        select * from {{ ref( 'scribe_user_contract_detailed' ) }}
    )

    , during_timestamps as (
        select during_end
            , lag(during_end)
                over (partition by user_id order by during_start)
                as previous_during_end
        from user_contracts
    )

select *
from during_timestamps
where during_end < previous_during_end
