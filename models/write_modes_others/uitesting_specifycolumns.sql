{{
  config({    
    "materialized": "incremental",
    "incremental_predicates": [
      "DBT_INTERNAL_SOURCE.c_tinyint > -10
      AND CASE
            WHEN DBT_INTERNAL_DEST.c_struct.city IS NOT NULL
              THEN true
            WHEN DBT_INTERNAL_SOURCE.c_struct.pin IS NOT NULL
              THEN false
            WHEN DBT_INTERNAL_SOURCE.c_string NOT IN ({{v_model_string_real}}, {{ var('v_project_string_real') }})
              THEN true
            WHEN (1 != 2)
            or (1 < 2)
            or (2 <= 2)
            or (2 <=> 2)
            or ((2 % 1.8) == 1)
            or (to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52'))
            or (add_months('2016-08-31', 1) < add_months('2017-08-31', 3))
            or (true and false)
            or array_contains(array_distinct(array(1, 2, 3)), 2)
            or array_contains(array_except(array(1, 2, 3), array(1, 3, 5)), 2)
            or array_contains(array_intersect(array(1, 2, 3), array(1, 3, 5)), 10)
            or (array_join(array('hello', 'world'), ' ', ',') LIKE '%hello%')
            or (array_max(array(1, 20, 3)) > 10)
            or (array_min(array(1, 20, 3)) > 20)
            or array_contains(array_remove(array(1, 2, 3, 3), 3), 3)
            or array_contains(array_repeat(5, 2), 6)
            or array_contains(array_union(array(1, 2, 3), array(1, 3, 5)), 10)
            or arrays_overlap(array(1, 2, 3), array(3, 4, 5))
            or (10 BETWEEN 2 AND 20)
            or contains('Spark SQL', 'Spark')
            or endswith('Spark SQL', 'SQL')
            or (
                 EXISTS(
                   array(1, 2, 3), 
                   x -> x % 2 == 0)
               )
            or array_contains(filter(
                 array(1, 2, 3), 
                 x -> x % 2 == 1), 5)
            or array_contains(flatten(array(array(1, 2), array(3, 4))), 10)
            or forall(
                 array(1, 2, 3), 
                 x -> x % 2 == 0)
            or ilike('Spark', '_Park')
            or (1 IN (2, 3, 4))
            or (isnan(CAST('NaN' AS double)))
            or isnotnull(1)
            or isnull(1)
            or array_contains(json_object_keys('{\"key\": \"value\"}'), 'key1')
            or like('Spark', '_park')
            or map_contains_key(map(1, 'a', 2, 'b'), 1)
            or map_contains_key(map_concat(map(1, 'a', 2, 'b'), map(3, 'c')), 4)
            or map_contains_key(map_filter(
                 map(1, 0, 2, 2, 3, -1), 
                 (k, v) -> k > v), 3)
            or map_contains_key(map_from_arrays(array(1.0, 3.0), array('2', '4')), 2)
            or map_contains_key(map_from_entries(array(struct(1, 'a'), struct(2, 'b'))), 1)
            or array_contains(map_keys(map(1, 'a', 2, 'b')), 2)
            or array_contains(map_values(map(1, 'a', 2, 'b')), 'a')
            or map_contains_key(map_zip_with(
                 map(1, 'a', 2, 'b'), 
                 map(1, 'x', 2, 'y'), 
                 (k, v1, v2) -> concat(v1, v2)), 1)
            or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
            or (NOT true)
            or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100')
            or array_contains(sequence(1, 5), 4)
            or array_contains(shuffle(array(1, 20, 3, 5)), 10)
            or array_contains(slice(array(1, 2, 3, 4), 2, 2), 4)
            or array_contains(sort_array(array('b', 'd', 'c', 'a'), true), 'b')
            or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one')
            or startswith('Spark SQL', 'Spark')
            or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a')
            or array_contains(transform(
                 array(1, 2, 3), 
                 x -> x + 1), 1)
            or map_contains_key(transform_keys(
                 map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
                 (k, v) -> k + 1), 1)
            or map_contains_key(transform_values(
                 map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
                 (k, v) -> v + 1), 2)
            or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a')
            or xpath_boolean('<a><b>1</b></a>', 'a/b')
            or array_contains(zip_with(
                 array(1, 2), 
                 array(3, 4), 
                 (x, y) -> x + y), 1) IS NULL = true != true != false != true != true
              THEN true
            WHEN (
              (1 != 2)
              or (1 < 2)
              or (2 <= 2)
              or (2 <=> 2)
              or ((2 % 1.8) == 1)
              or (to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52'))
              or (add_months('2016-08-31', 1) < add_months('2017-08-31', 3))
              or (true and false)
              or array_contains(array_distinct(array(1, 2, 3)), 2)
              or array_contains(array_except(array(1, 2, 3), array(1, 3, 5)), 2)
              or array_contains(array_intersect(array(1, 2, 3), array(1, 3, 5)), 10)
              or (array_join(array('hello', 'world'), ' ', ',') LIKE '%hello%')
              or (array_max(array(1, 20, 3)) > 10)
              or (array_min(array(1, 20, 3)) > 20)
              or array_contains(array_remove(array(1, 2, 3, 3), 3), 3)
              or array_contains(array_repeat(5, 2), 6)
              or array_contains(array_union(array(1, 2, 3), array(1, 3, 5)), 10)
              or arrays_overlap(array(1, 2, 3), array(3, 4, 5))
              or (10 BETWEEN 2 AND 20)
              or contains('Spark SQL', 'Spark')
              or endswith('Spark SQL', 'SQL')
              or (
                   EXISTS(
                     array(1, 2, 3), 
                     x -> x % 2 == 0)
                 )
              or array_contains(filter(
                   array(1, 2, 3), 
                   x -> x % 2 == 1), 5)
              or array_contains(flatten(array(array(1, 2), array(3, 4))), 10)
              or forall(
                   array(1, 2, 3), 
                   x -> x % 2 == 0)
              or ilike('Spark', '_Park')
              or (1 IN (2, 3, 4))
              or (isnan(CAST('NaN' AS double)))
              or isnotnull(1)
              or isnull(1)
              or array_contains(json_object_keys('{\"key\": \"value\"}'), 'key1')
              or like('Spark', '_park')
              or map_contains_key(map(1, 'a', 2, 'b'), 1)
              or map_contains_key(map_concat(map(1, 'a', 2, 'b'), map(3, 'c')), 4)
              or map_contains_key(map_filter(
                   map(1, 0, 2, 2, 3, -1), 
                   (k, v) -> k > v), 3)
              or map_contains_key(map_from_arrays(array(1.0, 3.0), array('2', '4')), 2)
              or map_contains_key(map_from_entries(array(struct(1, 'a'), struct(2, 'b'))), 1)
              or array_contains(map_keys(map(1, 'a', 2, 'b')), 2)
              or array_contains(map_values(map(1, 'a', 2, 'b')), 'a')
              or map_contains_key(map_zip_with(
                   map(1, 'a', 2, 'b'), 
                   map(1, 'x', 2, 'y'), 
                   (k, v1, v2) -> concat(v1, v2)), 1)
              or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
              or (NOT true)
              or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100')
              or array_contains(sequence(1, 5), 4)
              or array_contains(shuffle(array(1, 20, 3, 5)), 10)
              or array_contains(slice(array(1, 2, 3, 4), 2, 2), 4)
              or array_contains(sort_array(array('b', 'd', 'c', 'a'), true), 'b')
              or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one')
              or startswith('Spark SQL', 'Spark')
              or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a')
              or array_contains(transform(
                   array(1, 2, 3), 
                   x -> x + 1), 1)
              or map_contains_key(transform_keys(
                   map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
                   (k, v) -> k + 1), 1)
              or map_contains_key(transform_values(
                   map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
                   (k, v) -> v + 1), 2)
              or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a')
              or xpath_boolean('<a><b>1</b></a>', 'a/b')
              or array_contains(zip_with(
                   array(1, 2), 
                   array(3, 4), 
                   (x, y) -> x + y), 1) IS NULL = true != true != false != true != true
            ) IS NOT NULL
              THEN true
            ELSE false
          END"
    ],
    "incremental_strategy": 'merge',
    "merge_update_columns": ['c_tinyint', 'c_smallint', 'c_int'],
    "on_schema_change": 'sync_all_columns',
    "unique_key": ['c_id']
  })
}}

{% set v_model_int_real = 22 %}
{% set v_model_string_real = "modelstr" %}


WITH all_type_non_partitioned_1 AS (

  SELECT * 
  
  FROM {{ source('hive_metastore.qa_database', 'all_type_non_partitioned') }}

),

all_type_non_partitioned_columns AS (

  {#Retrieves a comprehensive dataset of various data types for analysis.#}
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
    monotonically_increasing_id() AS c_id,
    CURRENT_DATE AS c_date,
    CURRENT_TIMESTAMP() AS c_timestamp
  
  FROM all_type_non_partitioned_1 AS in0

)

SELECT *

FROM all_type_non_partitioned_columns
