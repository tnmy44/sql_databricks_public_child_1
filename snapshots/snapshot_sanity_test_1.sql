{% snapshot snapshot_sanity_test_1 %}
{{
  config({    
    "check_cols": ['c_bigint', 'c_float'],
    "strategy": 'check',
    "target_schema": 'QA_SCHEMA',
    "unique_key": 'c_int'
  })
}}

WITH sanity_simple_model_1 AS (

  SELECT *
  
  FROM {{ ref('sanity_simple_model_1')}}

),

Reformat_1 AS (

  SELECT *
  
  FROM sanity_simple_model_1 AS in0

),

OrderBy_1 AS (

  SELECT *
  
  FROM Reformat_1 AS in0
  
  ORDER BY c_int ASC, c_string DESC

)

SELECT *

FROM OrderBy_1

{% endsnapshot %}
