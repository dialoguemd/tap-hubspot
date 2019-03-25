with
	apt_booking as (
        select * from {{ ref('careplatform_appointment_booking_started') }}
    )

select episode_id
	, min(timestamp_est) as appointment_booking_first_started_at
from apt_booking
group by 1
