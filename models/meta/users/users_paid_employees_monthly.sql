with users as (
        select * from pdt.users
    )

    , organizations as (
        select * from {{ ref( 'organizations' ) }}
    )

	, months as (
        select date_trunc('month', 
            generate_series(current_date - interval '6 months', 
                            current_date, 
                            interval '1 month')
                           ) as date_month
    )

    select months.date_month
        , organizations.organization_name
        , organizations.account_name
        , count(users.*) as count_paid_employees
    from months
    left join users 
        on (months.date_month + interval '1 month') > users.created_at
        and (users.deactivated_at > months.date_month or users.deactivated_at is null)
        and is_employee
    inner join organizations using (organization_id)
    group by 1,2,3
