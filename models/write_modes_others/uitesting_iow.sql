{{
  config({    
    "materialized": "incremental",
    "incremental_strategy": 'insert_overwrite',
    "on_schema_change": 'append_new_columns',
    "partition_by": ['c_string', 'c_boolean', 'c_struct.state']
  })
}}

WITH all_type_non_partitioned AS (

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
  
  FROM all_type_non_partitioned AS in0

)

SELECT *

FROM all_type_non_partitioned_columns
