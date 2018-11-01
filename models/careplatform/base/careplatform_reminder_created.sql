select timestamp as created_at
    , reminder_id
    , reminder_episode_id as episode_id
    , reminder_due_at as due_at
from careplatform.reminders_create_new_success
