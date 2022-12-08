{{ config(materialized='table') }}

with tab1 as (
    select * from {{ source('stagging_trip', 'trip') }}
    ),
     final as (
    select * from tab1
    )
select CAST(COALESCE(VendorID, 4) AS integer) AS VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, COALESCE(passenger_count, 2) AS passenger_count, trip_distance, COALESCE(RatecodeID, 99) AS RatecodeID,
        COALESCE(store_and_fwd_flag, FALSE) AS store_and_fwd_flag, PULocationID, DOLocationID, COALESCE(payment_type, 5) payment_type, fare_amount, extra, mta_tax, tip_amount,
        tolls_amount, improvement_surcharge, total_amount, COALESCE(congestion_surcharge, 0) as congestion_surcharge
from final
order by tpep_pickup_datetime

