{{
  config({    
    "materialized": "incremental",
    "incremental_predicates": ['c_tinyint > -1 AND c_int <= 10000 AND (c_struct.city IS NOT NULL AND c_struct.pin IS NOT NULL)'],
    "incremental_strategy": 'replace_where',
    "on_schema_change": 'append_new_columns'
  })
}}

WITH all_type_non_partitioned_1 AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_database', 'all_type_non_partitioned') }}

),

all_type_non_partitioned_columns AS (

  {#Extracts all non-partitioned columns from the 'all_type_non_partitioned' table.#}
  SELECT 
    c_tinyint AS c_tinyint,
    c_smallint AS c_smallint,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    monotonically_increasing_id() AS c_id
  
  FROM all_type_non_partitioned_1 AS in0

)

SELECT *

FROM all_type_non_partitioned_columns

{% if is_incremental() %}
  WHERE 
    c_string IS NOT NULL
{% endif %}