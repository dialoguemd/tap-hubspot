# Segment Tracking Guide

Segment tracking should aim to recreate every user's behaviour such that we can analyse on aggregate how a feature is being used, including the intent, outcome, and context.

## What gets tracked

When tracking events in our applications we are, in order of priority, interested in:

1. Tracking the occurence of the event
2. Capturing the additional properties of this event (including things like `timestamp`, `episode_id`, `used_id`)
3. Creating or using a unique identifier for use in analysis downstream (such as a `task_id`)

It may not be possible to do all three in all tracking situations, but respect the prioritization.

## Naming Conventions

#### Object-Action framework

- All events should be defined as `Object` and the `Action` being performed on the object by the user
- When there is any abiguity about who this user might be, pass an additional prop called `user_type` and indicate whether it is a `patient`, `practitioner`, `hr_admin`, or whatever else.
- The capital convention is all lower case, such as `user signed up` or `call started`

#### Properties Naming

- When an action occurs inside an episode, `episode_id` should always be passed as a property (and never as `channel_id`).
- When tracking an event, the python segment library always expects a `user_id`; this can be either that of the practitioner or the patient.
- When there is any ambiguity the type of the user should be clarified in a property called `user_type` with the value of `practitioner`, `patient`, `admin`, or other relevant user type.
- Additional properties will be indicated in the tracking spec.

## Unique Identifiers

- Segment will generate a unique identifier per event tracked which can be used for analysis downstream.
- Additional identifiers should be implemented when possible to help with analysis. The presence of an ID makes direct joining possible whereas without it an `episode_id` and series of `timestamp`s will be used to approximate the same join. When possible, feature IDs or workflow IDs should always be created and passed (e.g. `call_id` or `qnaire_tid`).

## Best Practices

- When possible, multiple events that occur in similar contexts should be flattened into one event. E.g. `call ended by practitioner`, `call ended by patient` could become `call ended` with a `type` of `ended_by_patient` or `ended_by_practitioner`.
- If you think of an additional property that could be passed, mention it to have it included in the spec. Adding more context will make analysis easier downstream.

## Further Reading and Segment Documentation:

- [Anatomy of a Track Call](https://segment.com/academy/collecting-data/the-anatomy-of-a-track-call/)
- [What's a tracking plan?](https://segment.com/docs/guides/best-practices/what-s-a-tracking-plan-and-why-should-you-care/)
- [Naming Conventions](https://segment.com/academy/collecting-data/naming-conventions-for-clean-data/)
- [Track Documentation](https://segment.com/docs/spec/track/)
