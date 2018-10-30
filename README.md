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
