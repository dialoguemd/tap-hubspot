## Catwalk Style Guide

### Model Configuration
* If a particular configuration applies to all models in a directory, it should be specified in the project

### Model Naming
* Models based on events should conform to the segment naming convention of object-action, e.g. `careplatform_reminder_created` (https://segment.com/academy/collecting-data/naming-conventions-for-clean-data/#the-object-action-framework)
* Models should have folder prefixes before their actual names up to two levels deep; when a third folder layer is present, use only the first and last folder levels(e.g. for `tableau/medops/medops_scorecard/file` the file name is `tableau_medops_scorecard_chats.sql`, not `tabelau_medops_chats.sql` or `tableau_medops_medops_scorecard_chats.sql`)
    * Folder names that are excluded from this convention include `meta`, `base`, `transform`, and `table`. These should never appear in a model's name
* Business objects, exceptionally, require no folder prefixes in their names and are stored in `meta/table`
* All seeds should be prefixed with `data_` and should have their own base model in the most relevant folder; this base model should then be identified as a seed base model in its `schema.yml`

#### Field Naming Conventions
* ID and Name fields are always prefixed by the object name
* In models that include multiple timestamps, use the `action_at` naming convention (e.g. `created_at`, `activated_at`)
* `timestamp` is the naming convention for event timestamps when in a base model
* `date_day`, `date_month`, etc. is the naming convention for aggregated and time series data (as to not use the reserved `date` keyword)
* In models with multiple CTEs or sources, all fields should be prefixed with their source for readability and ease of debugging (e.g. `episode_kpis.ttr_total`)

#### Base Models
* Only base models should select from source tables / views
* Only a single base model should be able to select from a given source table / view.
* Base models should be placed in a base/ directory
* Base models should perform all necessary data type casting
* Base models should perform all field naming to force field names to conform to standard field naming conventions
* Source fields that use reserved words must be renamed in base models

#### Meta Models
* Models that depend on other models are stored within the `meta` folder and then in the folder corresponding to their subject

#### CTEs
* All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file, like python `import`
* Where performance permits, CTEs should perform a single, logical unit of work
* CTE names should be as verbose as needed to convey what they do
* CTEs with confusing or notable logic should be commented
* CTEs that are duplicated across models should be pulled out into their own models
* CTEs should be formatted like this:
```
with
events as (

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
with
my_data as (
    select * from {{ ref('my_data') }}
)

, some_cte as (
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
* Any failing tests should be fixed or explained prior to requesting a review
