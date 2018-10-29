## Catwalk Style Guide

### Model Configuration and Naming Conventions
* If a particular configuration applies to all models in a directory, it should be specified in the project

#### Field Naming Conventions
* ID and Name fields are always prefixed by the object name
* `timestamp` is the time that the event occurred, the data originated, etc; no other timestamps should be present in raw data
* `blank_at` is the form for timestamps when in a transformed model (e.g. `created_at`, `activated_at`)

#### Base Models
* Only base models should select from source tables / views
* Only a single base model should be able to select from a given source table / view.
* Base models should be placed in a base/ directory
* Base models should perform all necessary data type casting
* Base models should perform all field naming to force field names to conform to standard field naming conventions
* Source fields that use reserved words must be renamed in base models

#### CTEs
* All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file, like python `import`
* Where performance permits, CTEs should perform a single, logical unit of work.
* CTE names should be as verbose as needed to convey what they do
* CTEs with confusing or noteable logic should be commented
* CTEs that are duplicated across models should be pulled out into their own models
* CTEs should be formatted like this:
```
with events as (

	...

),

-- CTE comments go here
filtered_events as (

	...

)

select * from filtered_events
```

---

### SQL Style Guide
* Indents should be four spaces (except for predicates, which should line up with the where keyword)
* Lines of SQL should be no longer than 80 characters
* Field names and function names should all be lowercase
* The `as` keyword should be used when projecting a field or table name
* Ordering and grouping by a number (eg. group by 1, 2) is ok
* When possible, take advantage of `using` in joins
* Prefer union all to union *
* DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEWLINES ARE CHEAP, BRAIN TIME IS EXPENSIVE

#### Example Code
```
with my_data as (

    select * from {{ ref('my_data') }}

),

some_cte as (

    select * from {{ ref('some_cte') }}

)

select [distinct]
    field_1,
    field_2,
    field_3,
    case
        when cancellation_date is null and expiration_date is not null then expiration_date
        when cancellation_date is null then start_date+7
        else cancellation_date
    end as canellation_date

    sum(field_4),
    max(field_5)

from my_data
join some_cte using (id)

where field_1 = ‘abc’
  and (
    field_2 = ‘def’ or
    field_2 = ‘ghi’
  )

group by 1, 2, 3
having count(*) > 1
```

---

### Testing (to be reviewed later)
* Every model should be tested in a schema.yml file
* At minimum, unique and foreign key constraints should be tested (if applicable)
* The output of dbt test should be pasted into PRs
* Any failing tests should be fixed or explained prior to requesting a review