{{ config(materialized='table') }}

with tab1 as (
    select * from {{ source('stagging_trip', 'trip') }}
    )
select DISTINCT RatecodeID, 
        CASE WHEN RatecodeID=1 THEN 'Standard rate'
             WHEN RatecodeID=2 THEN 'JFK'
             WHEN RatecodeID=3 THEN 'Newark'
             WHEN RatecodeID=4 THEN 'Nassau or Westchester'
             WHEN RatecodeID=5 THEN 'Negotiated fare'
             WHEN RatecodeID=6 THEN 'Group ride'
             WHEN RatecodeID=99 THEN 'Unknown'
        END AS ratecode
from tab1
WHERE RatecodeID is not null
order by RatecodeID