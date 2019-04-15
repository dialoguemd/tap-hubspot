# The schema here indicates within which segment source the event is tracked
# such as the careplatform or countdown

# The event_completed is the "object action" name that was tracked such as
# "button clicked" for careplatform or "questionnaire started" in countdown

select * from schema.event_completed
