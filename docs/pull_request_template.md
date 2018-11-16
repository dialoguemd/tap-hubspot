## PR Creator Checklist:
### DBT Checklist:
 - [ ] Meets style guidelines (https://github.com/dialoguemd/catwalk/blob/master/style_guide.md)
 - [ ] Model-naming conventions are met
 - [ ] All models dependent on other models outside of their folder are in `meta` or `tableau`
 - [ ] Tests have been written for new models and updated as needed for existing models
 - [ ] Tests pass for all new models, modified models, and children of models

### Extra: Tableau Dashboards:
 - [ ] Tableau is pointing at `analytics` not a dev schema
 - [ ] Tableau data sources are set as extracts with refresh schedules before 9am
 - [ ] Link to Tableau Dashboard added: _____________________

### Extra: Metabase Tables:
 - [ ] Materialized as a table
 - [ ] Test question added to Metabase
 - [ ] Link to Metabase Question / Dashboard added: _____________________
