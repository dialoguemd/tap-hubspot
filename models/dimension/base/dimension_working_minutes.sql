with series as (
  select generate_series(0, 
    (extract(days from timezone('America/Montreal', '2021-01-01 00:00:00')
      - timezone('America/Montreal', '2018-01-01 00:00:00'))::integer
      * 1440) ) as n
)

select 
  (timezone('America/Montreal', '2018-01-01 00:00:00')
  + n * interval '1min') as minute
from series
where extract(isodow from (timezone('America/Montreal', '2018-01-01 00:00:00')
        + (n || ' minutes')::interval)) < 6
      and (timezone('America/Montreal', '2018-01-01 00:00:00')
        + n * interval '1min')::time >= '09:00'::time
      and (timezone('America/Montreal', '2018-01-01 00:00:00')
        + n * interval '1min')::time <  '17:00'::time
      and date_trunc('day', (timezone('America/Montreal', '2018-01-01 00:00:00')
        + n * interval '1min'))
        not in ('2018-01-01',
                '2018-03-30',
                '2018-04-02',
                '2018-05-21',
                '2018-06-24',
                '2018-07-01',
                '2018-09-03',
                '2018-10-08',
                '2018-12-25'
         )
