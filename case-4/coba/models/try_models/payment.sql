{{ config(materialized='table') }}

with tab1 as (
    select * from {{ source('stagging_trip', 'trip') }}
    )
select DISTINCT payment_type, 
        CASE WHEN payment_type=1 THEN 'Credit card'
             WHEN payment_type=2 THEN 'Cash'
             WHEN payment_type=3 THEN 'No charge'
             WHEN payment_type=4 THEN 'Dispute'
             WHEN payment_type=5 THEN 'Unknown'
        END AS details
from tab1
WHERE payment_type is not null
order by payment_type