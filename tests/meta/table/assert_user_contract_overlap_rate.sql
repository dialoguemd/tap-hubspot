with user_contracts as (
        select * from {{ ref( 'scribe_user_contract_detailed' ) }}
    )

    , overlap as (
        select user_contracts.user_id
            , user_contracts.during_start
            , user_contracts.contract_id
            , overlapping.contract_id as overlapping_contract_id
        from user_contracts
        left join user_contracts as overlapping
            on user_contracts.user_id = overlapping.user_id
            and user_contracts.during && overlapping.during
    )

    , users_with_overlap as (
        select user_id
            , contract_id <> overlapping_contract_id as same_contract
            , min(during_start) as first_during_start
        from overlap
        -- Check if the contract overlaps with any other contracts excluding itself
        where overlapping_contract_id is not null
        group by 1,2
    )

    , overlap_rate as (
        select date_trunc('month', first_during_start) as month
            , count(*) as count_total
            , count(*) filter (where same_contract) as count_overlap
            , count(*) filter (where same_contract) * 1.0 / count(*) as overlap_fraction
        from users_with_overlap
        group by 1
    )

select * from overlap_rate
-- Calibrate on 2019-02-01 with a max rate of 0.0049 in 2017-05-01 for populations > 1000
where count_total > 1000
    and overlap_fraction > 0.005
