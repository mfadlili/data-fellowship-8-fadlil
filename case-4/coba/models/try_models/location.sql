{{ config(materialized='table') }}

with tab1 as (
    select * from {{ source('stagging_trip', 'location') }}
    ),
     final as (
    select * from tab1
    )
select *
from final