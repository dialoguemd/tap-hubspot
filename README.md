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

- 05:30 EST / 00:30 UTC ==== Lambdas ETL
- 05:45 EST / 00:30 UTC ==== Segment
- 06:30 EST / 01:30 UTC ==== Sinter
- 07:30 EST / 02:30 UTC ==== Tableau

**Afternoon**

- 12:00 EST / 07:00 UTC ==== Lambdas ETL
- 12:15 EST / 07:15 UTC ==== Segment
- 13:00 EST / 08:00 UTC ==== Sinter
- 14:00 EST / 09:00 UTC ==== Tableau
