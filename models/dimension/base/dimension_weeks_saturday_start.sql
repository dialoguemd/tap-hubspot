select generate_series(
'2016-01-02', -- the first saturday of 2016
current_date,
interval '1 week')
as date_week
