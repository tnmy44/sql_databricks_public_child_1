{% snapshot snapshot_env_uitesting_shared_mid_model_1 %}
{{
  config({    
    "check_cols": ['c_bigint', 'c_float'],
    "strategy": 'check',
    "target_schema": 'QA_SCHEMA',
    "unique_key": 'c_int'
  })
}}

WITH env_uitesting_shared_mid_model_1 AS (

  SELECT *
  
  FROM {{ ref('env_uitesting_shared_mid_model_1')}}

),

Reformat_1 AS (

  SELECT *
  
  FROM env_uitesting_shared_mid_model_1 AS in0

),

Limit_1 AS (

  SELECT *
  
  FROM Reformat_1 AS in0
  
  LIMIT 5

)

SELECT *

FROM Limit_1

{% endsnapshot %}
