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

- 05:30 UTC       / 01:30 EST / 00:30 EDT ==== Loading Phase (Lambdas)
- 06:00-07:00 UTC / 02:00 EST / 02:00 EDT ==== Loading Phase (Segment)
- 08:00 UTC       / 04:00 EST / 03:00 EDT ==== Transformation Phase (DBT)
- 09:00-10:00 UTC / 05:00 EST / 05:00 EDT ==== Refresh Phase (Tableau)

**Afternoon**

- 16:30 UTC       / 12:30 EST / 11:30 EDT ==== Loading Phase (Lambdas)
- 17:00-18:00 UTC / 13:00 EST / 13:00 EDT ==== Loading Phase (Segment)
- 19:00 UTC       / 15:00 EST / 14:00 EDT ==== Transformation Phase (DBT)
- 20:00-21:00 UTC / 16:00 EST / 16:00 EDT ==== Refresh Phase (Tableau)

Current schedule constraints:
- DBT Cloud can only run on the hour (paid plan allows for custom cron)
- Segment can only run on the hour
- Tableau and Segment schedule based on local time (which has daylight savings), Lambdas and DBT use UTC

## Archives

Some tables are archived using DBT to keep history of slowly changing dimension

By default, running an archive will write in `archive_dev`
A different suffix can be provided as a variable when calling `dbt archive`:

```dbt archive --vars 'schema_suffix: _dev2'```

To run in prod (without suffix) run the following command:
```dbt archive --vars 'schema_suffix: ""'```
