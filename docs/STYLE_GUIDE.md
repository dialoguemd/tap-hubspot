# Style Guide

This guide includes our conventions on:
* [**Model Configuration**](docs/style_guide.md#model-configuration)
* [**Field Naming Conventions**](docs/style_guide.md#field-naming-conventions)
* [**SQL Style**](docs/style_guide.md#sql-style)
* [**Jinja Templating**](docs/style_guide.md#jinja-templating)
* [**Seeds**](docs/style_guide.md#seeds)
* [**Testing**](docs/style_guide.md#testing)

## Model Configuration

### DBT Project File
* If a particular configuration applies to all models in a directory, it should be specified in the project file
* If a variable is useful in multiple files define it as a global variable in the project file

### Model Naming
* Models based on events should conform to the segment naming convention of object-action, e.g. `careplatform_reminder_created` (https://segment.com/academy/collecting-data/naming-conventions-for-clean-data/#the-object-action-framework)
* Models should have folder prefixes before their actual names up to two levels deep; when a third folder layer is present, use only the first and last folder levels(e.g. for `tableau/medops/medops_scorecard/file` the file name is `tableau_medops_scorecard_chats.sql`, not `tabelau_medops_chats.sql` or `tableau_medops_medops_scorecard_chats.sql`)
    * Folder names that are excluded from this convention include `meta`, `base`, `transform`, and `table`. These should never appear in a model's name
* Business objects, exceptionally, require no folder prefixes in their names and are stored in `meta/table`
* All seeds should be prefixed with `data_` and should have their own base model in the most relevant folder; this base model should then be identified as a seed base model in its `schema.yml`

### Hooks
* TODO: add documentation and handle access control with these

#### Base Models
* Only base models should select from source tables / views
* Only a single base model should be able to select from a given source table / view.
* Base models should be placed in a base/ directory
* Base models should perform all necessary data type casting
* Base models should perform all field naming to force field names to conform to standard field naming conventions
* Source fields that use reserved words must be renamed in base models

#### Meta Models
* Models that depend on other models are stored within the `meta` folder and then in the folder corresponding to their subject
* Aggregate timeseries models should be suffixed by their timeframe in the form of `_daily`, `_weekly`, `_monthly`, etc.
* Other aggregates should be suffixed by their aggregate dimension, e.g. `usage_daus_by_org_monthly`

#### CTEs
* All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file, like python `import`
* Where performance permits, CTEs should perform a single, logical unit of work
* CTE names should be as verbose as needed to convey what they do
* CTEs with confusing or notable logic should be commented
* CTEs that are duplicated across models should be pulled out into their own models
* In models with multiple CTEs or sources, all fields should be prefixed with their source for readability and ease of debugging (e.g. `episode_kpis.ttr_total`)
* CTEs should be formatted like this:
```
with
    events as (
        ...
    )

    -- CTE comments go here
    , filtered_events as (
        ...
    )

select *
from filtered_events
```

---

## Field Naming Conventions

#### Identifiers (IDs)
* ID and Name fields are always prefixed by the object name
* `patient_id` and `practitioner_id` should be used in place of user_id; we have two main user types and it can be confusing as to whom an event corresponds. Use these ids and you'll communicate not only the id but also the user_type. 
    * In Maestro you could also reasonably use an `admin_id` or similar

#### Dates and Timestamps
* `timestamp` is the naming convention for event timestamps when in a base model
* If a timestamp or date has been stripped of its timezone suffix it should be indicated in a field_name suffix (e.g. `timestamp_est`)
* In models that include multiple timestamps, use the `action_at` naming convention (e.g. `created_at`, `activated_at`)
* `date_day`, `date_month`, etc. is the naming convention for aggregated and time series data (as to not use the reserved `date` keyword)


#### Other
* Filtration should be indicated in the suffix of field names such as `first_message_care_team` and `first_message_patient`; if combined with aggregation the filtration suffix should preced the aggregation suffix
* Aggregation should be indicated in the suffix of field names such as `cost_avg`, `rt_sum`, `rt_count`, etc. ; aggregation suffixes should be used for all numerical columns and whenever there is possible ambiguity about a column's purpose

---

## SQL Style
* Indents should be four spaces (except for predicates, which should line up with the where keyword)
* Lines of SQL should be no longer than 80 characters
* Field names and function names should all be lowercase
* The `as` keyword should be used when projecting a field or table name
* Ordering and grouping by a number (eg. group by 1, 2) is ok
* When possible, take advantage of `using` in joins
* Prefer union all to union *
* Commas should lead lines not end them
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

## Jinja Templating
* `for` loops should be used anytime you're iterating on 3+ variables or if the logic is complex and repetitive for 2+
* `if` conditions should be used to facilitate incremental models and in conjunction with for loops as needed
* Variables should be `set` whenever there is a config variable present or a long piece of logic that will be used in multiple places
* DBT Utils should be used wherever possible and they should be documented inline for future reference (e.g. if you're using pivot for the first time explain what is going on and why)

---

## Seeds
* Seed CSVs are all stored in /data and should be named according to relevance
* All transformations should occur in the DBT Project file for seeds
* Base models for seeds should be simple `select * from` statements

---

## Testing
* Every model should be tested in a schema.yml file
* At minimum, unique and foreign key constraints should be tested (if applicable)
* Any failing tests should be fixed or explained prior to requesting a review
* Every Segment schema should be tested for recency using the DBT Utils package
* Data tests are organized in the `/tests` directory and in subdirectories matching to their source or model directory (e.g. `scribe` or `meta/costs`; due to the relatively small count of data tests there is no need to distinguish between base and transform)
* TODO: Bond and trend tests shold be used when possible

#### Static Historical Testing
Static Historical Testing compares a historical snapshot of metrics with computed metrics as they are at testing. This allows for a full-refresh to be tested appropriately and for core tables to be tested for changes in history (perhaps due to a change or corruption of a source data set).

They do this by grouping by various dimensions, calculating various facts, and then comparing those facts with the possibility of allowing for a degree of sensitivity (e.g. comparing DAUs within 5% of what they were historically for various organizations, this allows for small changes possibly to things like user_contracts while also asserting that historical reporting has remained roughly the same.)

These tests are configured by creating a snapshot CTAS model (stored in `analysis/static_historicals`) and a test file (stored in `tests/historical_equivalency`).

The CTAS model is not compiled and run by DBT, so it's plain old SQL. This model is formed by grouping by dimensions and summing or counting various facts, depending on their type (i.e. sum numerical values and count IDs).

```
CREATE TABLE
        static_historicals.chats
    AS (

    with
        data as (
            select * from analytics.chats
            where date_week < date_trunc('week', current_timestamp)
        )

    select date_week
        , chat_type
        , count(distinct user_id) as user_count
        , count(distinct episode_id) as episode_count
        , count(distinct patient_id) as patient_count
        , current_timestamp as archived_at
    from data
    group by 1,2
)
```

The test model is classic Jinja. Define the facts, dimensions, and then fill in the definition with them, the model name, and a sensitivity (recommended would be 0.05).

```
{% set facts = ['user', 'episode', 'patient'] %}

{% set dimensions = ['date_week', 'chat_type'] %}

{{ historical_equivalency_count_test(facts, dimensions, 'chats', 0.05) }}
```
