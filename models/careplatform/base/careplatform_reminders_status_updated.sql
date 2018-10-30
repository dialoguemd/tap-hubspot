select timestamp as updated_at
    , reminder_id
    , reminder_episode_id as episode_id
    , reminder_due_at as due_at
    , reminder_status
from careplatform.reminders_status_change_success
