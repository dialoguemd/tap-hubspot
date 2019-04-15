# [Feature Name]

### Description of Feature

With this feature you can save more lives.

Add a link to the JIRA epic here and any other documentation such as a notion page or mock-ups.

### Evaluation of Feature

The feature will be evaluated based on
1) both ATTR and TTR drop by 10% to help with our automation OKR
3) adoption rate among nurses and CCs is at least 50% daily; if not we will revisit the implementation of this feature
3) its error rate is lower than 1% of uses daily; if not we will revisit the implementation of this feature

**Note:** you can find definitions for any of these acronyms [here.](https://www.notion.so/godialogue/Metrics-definitions-aaeb1affa6f94093a16eaef05c476989)

### Feature Flow

Fill in the table below with the event names, their Segment sources (i.e. individual apps or backend services which will manifest as DB schemas when Segment tracks them), and the event descriptions. Each row in this table will map to a schema.yml file and a SQL model.

| Event Name                 | Source / Schema | Description                                                                                               |
|----------------------------|-----------------|-----------------------------------------------------------------------------------------------------------|
| phone_model_button_clicked | careplatform    | Practitioner launches call modal by clicking call button in the CP, launching the modal as its own window |
| phone_call_button_clicked  | careplatform    | Practitioner starts call by clicking phone button                                                         |
| capability_token_requested | telephone       | Telephone service requests request token from Twilio                                                      |
| call_config_received       | telephone       | Receive TwiML (call config) from Twilio                                                                   |
| call_ringing               | telephone       | Practitioner is connected and call to patient is ringing                                                  |
| call_started               | telephone       | Patient picks up and is connected                                                                         |
| call_ended                 | telephone       | Call is disconnected by patient, practitioner, or due to error                                            |
|                            |                 |                                                                                                           |
| client_error               | careplatform    | An error is sent from the client due to some error                                                        |

**Note:** the events are in a directed flow with the exception of `client_error` which can occur at any point.
