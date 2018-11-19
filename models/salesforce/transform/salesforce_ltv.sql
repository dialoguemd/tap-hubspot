select date_trunc('month', close_date) as date_month
    , sum(amount) as mrr_signed
    , coalesce(
        sum(amount)
            filter (where segment = '1000+')
        , 0) as mrr_signed_eae
    , coalesce(
        sum(amount)
            filter (where segment <> '1000+')
        , 0) as mrr_signed_ae
from {{ ref('salesforce_opportunities_detailed') }}
where is_won
group by 1
