{{ config(materialized='table') }}

with tab1 as (
    select * from {{ source('stagging_trip', 'trip') }}
    )
select DISTINCT VendorID, 
        CASE WHEN VendorID=1 THEN 'Creative Mobile Technologies, LLC'
             WHEN VendorID=2 THEN 'VeriFone Inc.'
             WHEN VendorID=4 THEN 'Unknown'
        END AS details
from tab1
WHERE VendorID is not null
order by VendorID