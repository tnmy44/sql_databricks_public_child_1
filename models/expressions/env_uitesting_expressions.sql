WITH all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Reformat_1 AS (

  SELECT 
    CAST(c_int AS string) AS customer_id,
    c_string AS first_name,
    c_string AS last_name
  
  FROM all_type_parquet AS in0

),

Reformat_2 AS (

  SELECT 
    customer_id AS customer_id,
    first_name AS first_name,
    last_name AS last_name,
    true AS c_expressions
  
  FROM Reformat_1 AS in0

),

SQLStatement_1 AS (

  SELECT cast(ANY (col1) FILTER (  
           WHERE col2 = 1
         ) AS string) AS c1
  
  FROM VALUES
        (false, 1),
        (false, 2),
        (true, 2),
        (NULL, 1) AS tab(col1, col2)
  
  UNION
  
  SELECT cast(ANY (col) AS string) AS c1
  
  FROM VALUES
        (true),
        (false),
        (false) AS tab(col)
  
  UNION
  
  SELECT CAST(approx_count_distinct(col1) AS string) AS c1
  
  FROM VALUES
        (1),
        (1),
        (2),
        (2),
        (3) AS tab(col1)
  
  UNION
  
  SELECT cast(approx_count_distinct(col1) FILTER (  
           WHERE col2 = 10
         ) AS string) AS c1
  
  FROM VALUES
        (1, 10),
        (1, 10),
        (2, 10),
        (2, 10),
        (3, 10),
        (1, 12) AS tab(col1, col2)
  
  UNION
  
  SELECT CAST(approx_percentile(col, array(0.5, 0.4, 0.1), 100) AS string) AS c1
  
  FROM VALUES
        (0),
        (1),
        (2),
        (10) AS tab(col)
  
  UNION
  
  SELECT CAST(approx_percentile(DISTINCT col, 0.5, 100) AS string) AS c1
  
  FROM VALUES
        (0),
        (6),
        (6),
        (7),
        (9),
        (10) AS tab(col)
  
  UNION
  
  SELECT CAST(array_agg(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (NULL),
        (1) AS tab(col)
  
  UNION
  
  SELECT CAST(array_agg(DISTINCT col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (NULL),
        (1) AS tab(col)
  
  UNION
  
  SELECT CAST(avg(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(try_avg(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(bit_and(col) AS string) AS c1
  
  FROM VALUES
        (3),
        (5) AS tab(col)
  
  UNION
  
  SELECT cast(bit_and(col) FILTER (  
           WHERE col < 6
         ) AS string) AS c1
  
  FROM VALUES
        (3),
        (5),
        (6) AS tab(col)
  
  UNION
  
  SELECT CAST(bit_or(col) AS string) AS c1
  
  FROM VALUES
        (3),
        (5) AS tab(col)
  
  UNION
  
  SELECT cast(bit_or(col) FILTER (  
           WHERE col < 8
         ) AS string) AS c1
  
  FROM VALUES
        (3),
        (5),
        (8) AS tab(col)
  
  UNION
  
  SELECT CAST(bit_xor(col) AS string) AS c1
  
  FROM VALUES
        (3),
        (3),
        (5) AS tab(col)
  
  UNION
  
  SELECT CAST(bool_and(col) AS string) AS c1
  
  FROM VALUES
        (true),
        (true),
        (true) AS tab(col)
  
  UNION
  
  SELECT CAST(bool_or(col) AS string) AS c1
  
  FROM VALUES
        (true),
        (false),
        (false) AS tab(col)
  
  UNION
  
  SELECT CAST(collect_list(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (NULL),
        (1) AS tab(col)
  
  UNION
  
  SELECT CAST(collect_set(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (NULL),
        (1) AS tab(col)
  
  UNION
  
  SELECT CAST(corr(c1, c2) AS string) AS c1
  
  FROM VALUES
        (3, 2),
        (3, 3),
        (3, 3),
        (6, 4) AS tab(c1, c2)
  
  UNION
  
  SELECT cast(corr(DISTINCT c1, c2) FILTER (  
           WHERE c1 != c2
         ) AS string) AS c1
  
  FROM VALUES
        (3, 2),
        (3, 3),
        (3, 3),
        (6, 4) AS tab(c1, c2)
  
  UNION
  
  SELECT CAST(count(*) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (5),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(count(*) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (5),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT cast(count(col) FILTER (  
           WHERE col < 10
         ) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (5),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(count_if(
           col % 2 = 0) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (0),
        (1),
        (2),
        (2),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(covar_pop(c1, c2) AS string) AS c1
  
  FROM VALUES
        (1, 1),
        (2, 2),
        (2, 2),
        (3, 3) AS tab(c1, c2)
  
  UNION
  
  SELECT CAST(covar_samp(c1, c2) AS string) AS c1
  
  FROM VALUES
        (1, 1),
        (2, 2),
        (2, 2),
        (3, 3) AS tab(c1, c2)
  
  UNION
  
  SELECT CAST(every(col) AS string) AS c1
  
  FROM VALUES
        (true),
        (true),
        (true) AS tab(col)
  
  UNION
  
  SELECT CAST(first(col, true) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(first_value(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(kurtosis(col) AS string) AS c1
  
  FROM VALUES
        (-10),
        (-20),
        (100),
        (100),
        (1000) AS tab(col)
  
  UNION
  
  SELECT CAST(last(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(last_value(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(max(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (50),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(max_by(x, y) AS string) AS c1
  
  FROM VALUES
        (('a', 10)),
        (('b', 50)),
        (('c', 20)) AS tab(x, y)
  
  UNION
  
  SELECT CAST(mean(DISTINCT col) AS string) AS c1
  
  FROM VALUES
        (1),
        (1),
        (2),
        (NULL) AS tab(col)
  
  UNION
  
  SELECT CAST(min(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (50),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(min_by(x, y) AS string) AS c1
  
  FROM VALUES
        (('a', 10)),
        (('b', 50)),
        (('c', 20)) AS tab(x, y)
  
  UNION
  
  SELECT CAST(percentile(col, 0.3) AS string) AS c1
  
  FROM VALUES
        (0),
        (10),
        (10) AS tab(col)
  
  UNION
  
  SELECT CAST(percentile_approx(col, 0.5, 100) AS string) AS c1
  
  FROM VALUES
        (0),
        (6),
        (7),
        (9),
        (10),
        (10),
        (10) AS tab(col)
  
  UNION
  
  SELECT CAST(regr_avgx(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(regr_avgy(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(regr_count(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 2),
        (2, 3),
        (2, 4) AS t(y, x)
  
  UNION
  
  SELECT CAST(regr_r2(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(skewness(col) AS string) AS c1
  
  FROM VALUES
        (-10),
        (-20),
        (100),
        (1000),
        (1000) AS tab(col)
  
  UNION
  
  SELECT CAST(some(col) AS string) AS c1
  
  FROM VALUES
        (true),
        (false),
        (false) AS tab(col)
  
  UNION
  
  SELECT CAST(std(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(stddev(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(stddev_pop(DISTINCT col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(stddev_samp(DISTINCT col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(sum(col) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (NULL) AS tab(col)
  
  UNION
  
  SELECT CAST(try_avg(DISTINCT col) AS string) AS c1
  
  FROM VALUES
        (1),
        (1),
        (2) AS tab(col)
  
  UNION
  
  SELECT CAST(try_sum(col) AS string) AS c1
  
  FROM VALUES
        (NULL),
        (10),
        (15) AS tab(col)
  
  UNION
  
  SELECT CAST(var_pop(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(var_samp(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(variance(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (3),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      dense_rank() OVER (PARTITION BY a ORDER BY b),
      rank() OVER (PARTITION BY a ORDER BY b),
      row_number() OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      ntile(2) OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      percent_rank(b) OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A1', 3),
          ('A1', 6),
          ('A1', 7),
          ('A1', 7),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      dense_rank() OVER (PARTITION BY a ORDER BY b),
      rank() OVER (PARTITION BY a ORDER BY b),
      row_number() OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      cume_dist() OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      lag(b) OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      lead(b) OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(a AS string) AS c1
  
  FROM (
    SELECT 
      a,
      b,
      nth_value(b, 2) OVER (PARTITION BY a ORDER BY b)
    
    FROM VALUES
          ('A1', 2),
          ('A1', 1),
          ('A2', 3),
          ('A1', 1) AS tab(a, b)
  )
  
  UNION
  
  SELECT CAST(num AS string) AS c1
  
  FROM (
    SELECT 
      explode(map(1, 'a', 2, 'b')) AS (num, val),
      'Spark'
  )
  
  UNION
  
  SELECT CAST(elem AS string) AS c1
  
  FROM (
    SELECT 
      explode_outer(array(10, 20)) AS elem,
      'Spark'
  )
  
  UNION
  
  SELECT CAST(name AS string) AS c1
  
  FROM (
    SELECT 
      name,
      age,
      count(*)
    
    FROM VALUES
          (2, 'Alice'),
          (5, 'Bob') AS people(age, name)
    
    GROUP BY CUBE(name, age)
  )
  
  UNION
  
  SELECT CAST(name AS string) AS c1
  
  FROM (
    SELECT 
      name,
      grouping(name),
      sum(age)
    
    FROM VALUES
          (2, 'Alice'),
          (5, 'Bob') AS people(age, name)
    
    GROUP BY CUBE(name)
  )
  
  UNION
  
  SELECT CAST(name AS string) AS c1
  
  FROM (
    SELECT 
      name,
      age,
      grouping_id(name, age),
      conv(CAST(grouping_id(name, age) AS STRING), 10, 2),
      avg(height)
    
    FROM VALUES
          (2, 'Alice', 165),
          (5, 'Bob', 180) AS people(age, name, height)
    
    GROUP BY CUBE(name, age)
  )
  
  UNION
  
  SELECT CAST(col1 AS string) AS c1
  
  FROM (
    SELECT 
      'hello' AS col1,
      stack(2, 1, 2, 3) AS (first, second),
      'world'
  )

),

Join_1 AS (

  SELECT 
    in0.customer_id AS customer_id,
    in0.first_name AS first_name,
    in0.last_name AS last_name,
    in0.c_expressions AS c_expressions,
    in1.c1 AS c1
  
  FROM Reformat_2 AS in0
  INNER JOIN SQLStatement_1 AS in1
     ON in0.customer_id != in1.c1

)

SELECT *

FROM Join_1
