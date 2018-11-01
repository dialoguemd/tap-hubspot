select date_trunc('month', close_date) as date_month
    , sum(amount) as mrr_signed
    , coalesce(
        sum(amount)
            filter (where segment = '1000+' and province = 'QC')
        , 0) as mrr_signed_eae_qc
    , coalesce(
        sum(amount)
            filter (where segment = '1000+' and province <> 'QC')
        , 0) as mrr_signed_eae_roc
    , coalesce(
        sum(amount)
            filter (where segment <> '1000+' and province = 'QC')
        , 0) as mrr_signed_ae_qc
    , coalesce(
        sum(amount)
            filter (where segment <> '1000+' and province <> 'QC')
        , 0) as mrr_signed_ae_roc
from {{ ref('salesforce_opportunities_detailed') }}
where is_won
group by 1
