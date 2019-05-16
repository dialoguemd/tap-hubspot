with
	question_replied as (
		select * from {{ ref('countdown_question_replied') }}
	)

select question_tid
    , qnaire_tid
    , replied_at
    , timestamp
    , user_id
    , episode_id
    , qnaire_name
    , question_name
    , reply_labels
    , reply_value_singular
    , reply_values
    , reply_value
    , question_category
    , reply_label_first as cc_code
    , reply_label_first <> 'cc_confirmation_other' as is_cc_confirmed
from question_replied
where qnaire_name = 'cc_confirmation'
	and question_name = 'cc_confirmation'
