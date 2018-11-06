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
                           ) as month
    )

    select months.month
        , organizations.organization_name
        , organizations.account_name
        , count(users.*) as count_paid_employees
    from months
    left join users 
        on (months.month + interval '1 month') > users.created_at
        and (users.deactivated_at > months.month or users.deactivated_at is null)
        and is_employee
    left join organizations using (organization_id)
    group by 1,2,3
