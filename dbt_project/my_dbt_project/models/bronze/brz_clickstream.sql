{{ config(materialized='table') }}

select 
    CAST(USERID AS STRING) as user_id,
    CAST(SessionID AS STRING) as session_id,
    TIMESTAMP(Timestamp) AS event_timestamp,
    CAST(EventType AS STRING) AS event_type,
    CAST(ProductID AS STRING) AS product_id,
    CAST(Amount AS STRING) AS amount,
    CAST(Outcome AS STRING) AS outcome
from {{ ref('clickstream') }}