{{ config(materialized='table') }}

select 
    CAST(prodcut_id AS STRING) AS product_id,
    CAST(manufacturer AS STRING) AS manufacturer,
    CAST(management_group AS STRING) AS management_group
from {{ ref('product_master') }}