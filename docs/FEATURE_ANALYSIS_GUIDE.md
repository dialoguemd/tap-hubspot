# Feature Analysis Guide

This guide walks you through what needs to be done when a feature analysis is needed. It is comprised of a Feature Flow Markdown file and possibly multiple modifications or new schema YML files. 

The markdown file is both the acceptance criteria for the analysis and a spec for the tracking that is to be implemented on the feature. A template can be found [here.](../blob/master/docs/feature_template.md)


#### Checklist

- [ ] Product Owner creates Feature Flow Markdown
- [ ] Product Owner and developers define events and tests in schema.yml files
- [ ] Data reviews the PR for these new files and rework is done as needed
- [ ] Feature is implemented
- [ ] Data completes feature analysis
- [ ] Feature analysis is presented to Product Owner and team at Review


#### Acceptance Criteria

A Markdown file needs to be written for the feature flow. This file is both the acceptance criteria for the analysis and a spec for the tracking that is to be implemented on the feature. 

The acceptance criteria should include how you will evaluate this feature once it's in production. This is comprised of 1) which metrics, 2) with which thresholds or goals, and 3) with which types of actions required if the threshold is not met.

1) Metrics can include things like ATTR (Active Time To Resolve), Adoption Rate, or a variety of others. See more about these and other metrics [here.](https://www.notion.so/godialogue/Metrics-definitions-aaeb1affa6f94093a16eaef05c476989)
2) A threshold or goal is the number associated with the metric (e.g. a Nurse adoption rate of 50% daily).
3) These thresholds can then be used for feature acceptance (i.e. if it's not met then something needs to be reworked) or information-purposes (e.g. to track how a feature affects an OKR; even if the effect is small we will not turn off the feature because of that finding)

**Note:** tracking is not always associated to evaluation metrics explicitly. Some tracking is associated with metrics for other teams such as Medops or for tracking failures and understanding user flows in the case of failures.


#### Tracking Spec

The tracking spec is the list of events needed for evaluating the above metrics and for tracking failures. Having additional events that will give context to an error is in general the best direction to go.

The events are defined in a table with columns for the `Event Name`,  `Schema`, `Description`, and `Properties`.

- Event Name: is the name of the event being tracked; it is also the name that will be used for the event itself and will be surfaced in the analytics DB.
- Schema: is the name of the service or app where the event will be tracked (e.g. `Careplatform`, `countdown`); this is also as the name indicates the schema name that will be associated with the event in the DB.
- Description: a quick description of what is happening when the event occurs
- Properties: a list of additional properties that are to be tracked with the event (e.g. `episode_id`, `type`)

More guidance about how to define these events can be found [here.]([here.](../blob/master/docs/segment_tracking_guide.md)


## Event Specs

The events are then defined in their respective schema files as determined by the schema column in the Feature Flow Markdown's table.

Each event should have its own model including a name, description, and list of key columns with optional column descriptions and optional tests on those columns. These files are doubly useful for us in that they define what is required for the implementation (e.g. `episode_id` is to be `not_null` but `user_type` does not have that constraint) and are used post-implementation as tests to continuously assert that the events tracked meet the constraint. An example model can be found [here](../blob/master/docs/example_model.sql) and a schema.yml template can be found [here.](../blob/master/docs/schema_template.yml)
