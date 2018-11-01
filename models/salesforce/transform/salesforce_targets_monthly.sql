with
    users_month_ae_eae as (
        select *
        from {{ ref('salesforce_users_month')}}
        where title in ('Enterprise Account Executive', 'Account Executive')
    )

    , opportunities_direct_won as (
        select amount
            , number_of_employees
            , owner_id
            , case
            -- adjustment of JFT's sales closed in 2017 set to start in 2018
            -- these deals where counted in his 2018 quota
                when owner_id = '0056A000000qL2TQAU'
                    and close_date < '2018-01-01'
                    and i_date >= '2018-01-01'
                then '2018-01-01'
                else close_date
            end as close_date
        from {{ ref('salesforce_opportunities_detailed_direct') }}
        where stage_name = 'Closed Won'
    )

    , users_month_rank_tmp as (
        select user_id
            , user_name
            , date_month
            , started_date
            , last_login_date
            , city
            , state_code
            , country
            , country_code
            , title
            , department
            , row_number() over(partition by user_id order by date_month) as month_since_start
        from users_month_ae_eae
    )

    , users_month_rank as (
        select user_id
            , user_name
            , date_month
            , started_date
            , last_login_date
            , city
            , state_code
            , country
            , country_code
            , title
            , department
            , case
                when user_name = 'Jean-François Théorêt' then 15000
                when user_name = 'Mike Latty' then 10000
                when user_name = 'Steve Chamberlain' then 8000
                else 4200 end as target
            , case
                when extract('day' from started_date) > 15 and month_since_start > 1
                then month_since_start - 1
                else month_since_start
              end as month_since_start
        from users_month_rank_tmp
    )

    , users_target as (
        select user_id
            , user_name
            , date_month
            , started_date
            , last_login_date
            , city
            , state_code
            , country
            , country_code
            , title
            , department
            , target
            , month_since_start
            , month_since_start < 5 as is_ramp
            , case
                when month_since_start <= 1 then 0
                when month_since_start = 2 then .1 * target
                when month_since_start = 3 then .33 * target
                when month_since_start = 4 then .66 * target
                else target
            end as mrr_target
        from users_month_rank
    )

select users_target.user_id
    , users_target.user_name
    , users_target.date_month
    , users_target.started_date
    , users_target.last_login_date
    , users_target.city
    , users_target.state_code
    , users_target.country
    , users_target.country_code
    , users_target.title
    , users_target.department
    , users_target.target
    , users_target.month_since_start
    , users_target.is_ramp
    , users_target.mrr_target
    , coalesce(sum(opportunities_direct_won.amount), 0) as mrr_signed
    , coalesce(sum(opportunities_direct_won.number_of_employees), 0) as number_of_employees
from users_target
left join opportunities_direct_won
    on users_target.user_id = opportunities_direct_won.owner_id
        and users_target.date_month = date_trunc('month', opportunities_direct_won.close_date)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
