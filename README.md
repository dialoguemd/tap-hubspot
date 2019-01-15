# Catwalk

## Modelling for Dialogue's Analytics Database, powered by DBT

___

Catwalk is where we model. This repo uses dbt to create a modelling layer on 
top of our analytics data warehouse giving us better visibility on 
dependencies, more coverage and automation in testing, and a quicker way to
develop data models and visualizations.

---

- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [Installation](https://dbt.readme.io/docs/installation)
- Join the [chat](http://ac-slackin.herokuapp.com/) on Slack for live questions and support.

---

## Documentation

Run the following to generate and serve the documentation site:
```
dbt docs generate && dbt docs serve
```


## Conventions

TODO: difference between `base`, `table`, `transform`

## Run DBT in prod


1. Set a target in your profile pointing at prod (`analytics` schema)
```
dialogue:
  outputs:
    prod:
      type: postgres
      threads: 8
      host: analytics.cot2yvki2gdh.ca-central-1.rds.amazonaws.com
      port: 5432
      user: [username]
      pass: [password]
      dbname: analytics
      schema: analytics
```

2. Run
```
dbt seed --full-refresh --target prod && dbt run --target prod
```

## Data pipeline refresh schedule

**Morning**

- 05:00 UTC / 00:00 EST ==== Lambdas sending events to Segment
- 05:30 UTC / 00:30 EST ==== Lambdas ETL
- 06:00 UTC / 01:00 EST ==== Segment
- 07:00 UTC / 02:00 EST ==== Sinter
- 08:00 UTC / 03:00 EST ==== Tableau

**Afternoon**

- 17:00 UTC / 12:00 EST ==== Segment
- 17:30 UTC / 12:30 EST ==== Lambdas ETL
- 18:00 UTC / 13:00 EST ==== Sinter
- 19:00 UTC / 14:00 EST ==== Tableau

Current schedule constraints:
- Sinter can only run on the hour (paid plan allows for custom cron)
- Segment can only run on the hour

## Archives

Some tables are archived using DBT to keep history of slowly changing dimension

By default, running an archive will write in `archive_dev`
A different suffix can be provided as a variable when calling `dbt archive`:

```dbt archive --vars 'schema_suffix: _dev2'```

To run in prod (without suffix) run the following command:
```dbt archive --vars 'schema_suffix: ""'```
