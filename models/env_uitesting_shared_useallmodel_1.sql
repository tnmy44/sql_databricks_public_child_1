{{
  config({    
    "materialized": "table",
    "post_hook": [],
    "pre_hook": []
  })
}}

{% set v_float = 10.12 %}
{% set v_int_list = [1, 2, 3, 4, 5, 6] %}
{% set v_boolean = True %}
{% set v_dict = { "a": 2, "b": "hello" } %}
{% set v_expression = 'concat(c_struct.city, c_string)' %}
{% set v_int = 22 %}






WITH all_type_parquet AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_parquet') }}

),

Reformat_1_1 AS (

  SELECT 
    CAST(c_int AS string) AS customer_id,
    c_string AS first_name,
    c_string AS last_name
  
  FROM all_type_parquet AS in0

),

Reformat_2_1 AS (

  SELECT 
    customer_id AS customer_id,
    first_name AS first_name,
    last_name AS last_name,
    CAST(customer_id AS int) > 5
    and CAST(customer_id AS int) != 0
    or first_name LIKE '%A%'
    or ((1 & 1) == 1)
    or ((2 | 2) == 2)
    and 10 * CAST(customer_id AS int) == 20
    and last_name != first_name
    and (CAST(customer_id AS int) BETWEEN 10 and 20)
    and (array(10, 20, 30)[2] == 30)
    and (map(1, 'Hello', 2, 'World')[1] == 'Hello')
    and (10 ^ 20 == 50)
    and (map('three', 3).four == NULL)
    and (named_struct('a', 5, 'b', 'Spark').a == 5)
    and (1 = 2 and 1 == 2)
    and (1 >= 2 and 1 <= 2 and 1 != 3 or 2 > 4 or 4 < 5)
    and (
          EXISTS (
            array(1, NULL, 3),
            
            x -> x % 2 == 0
          )
        )
    or ilike('Spark', '_PARK')
    or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
    and (1 IS DISTINCT FROM 5)
    and ('invalid' IS false)
    and isnull(1)
    and ('t' IS NOT true)
    and (1 <=> '1')
    and (1 <> CAST(customer_id AS int))
    and (10 - 2 + 10 == 8)
    and (NOT true)
    and (false or true)
    and (2 % 0 == 0)
    and ('Spark' || 'SQL' == 'SparkSQL')
    and (r'%SystemDrive%\Users\John' LIKE r'%SystemDrive%\\Users%')
    and (r'%SystemDrive%\Users\John' RLIKE '%SystemDrive%\\\\Users.*')
    and (regexp_like('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*'))
    and (2L / 2L == 10)
    and (~ 0 == -1)
    and (base64(aes_encrypt('Spark', 'abcdefghijklmnop')) == NULL)
    and (CAST(aes_decrypt(unbase64('4A5jOAh9FNGwoMeuJukfllrLdHEZxA2DyuSQAWz77dfn'), 'abcdefghijklmnop') AS STRING) == NULL)
    and (ascii('234') == 234)
    and (base64('Spark SQL') == NULL)
    and (bin(13) == NULL)
    and (binary('Spark SQL') == NULL)
    and (btrim('abcaabaSparkSQLabcaaba', 'abc') != NULL)
    and (char(65) != NULL)
    and (char_length('Spark SQL ') == 10)
    and (character_length('Spark SQL ') == 10)
    and (concat(customer_id, 'hello') != NULL)
    and (concat_ws(',', 'Spark', array('S', 'Q', NULL, 'L'), NULL) != NULL)
    and contains('SparkSQL', 'Spork')
    and (crc32('Spark') > 0)
    and chr(65) != NULL
    and (3 ^ 5 == 6)
    and (hex(encode('Spark SQL', 'US-ASCII')) != NULL)
    and endswith('SparkSQL', 'SQL')
    and (find_in_set('ab', 'abc,b,ab,c,def') == 2)
    and (format_number(12332.123456, 4) == 2)
    and (format_number(12332.123456, '#.###') == 10)
    and (format_string('Hello World %d %s', 100, 'days') != NULL)
    and (hex('Spark SQL') != NULL)
    and (r'%SystemDrive%\Users\John' LIKE '%SystemDrive%\\\\Users%')
    and (initcap('sPark sql') != NULL)
    and (instr('SparkSQL', 'R') == 2)
    and (lcase('LowerCase') != NULL)
    and (LEFT('Spark SQL', 3) != NULL)
    and (length('Spark SQL ') > 20)
    and (levenshtein('kitten', 'sitting') > 10)
    and (locate('bar', 'abcbarbar') > 2)
    and (locate('bar', 'abcbarbar', 5) > 2)
    and (lower('LowerCase') != NULL)
    and (lpad('hi', 1, '??') != NULL)
    and (hex(lpad(x'1020', 5, x'05')) != NULL)
    and (('+' || ltrim('abc', 'acbabSparkSQL   ') || '+') != NULL)
    and (md5('Spark') != NULL)
    and (octet_length('Spark SQL') != NULL)
    and (parse_url('http://spark.apache.org/path?query=1', 'HOST') != NULL)
    and (position('bar', 'abcbarbar') > 10)
    and (position('bar', 'abcbarbar', 5) > 5)
    and (printf('Hello World %d %s', 100, 'days') != NULL)
    and (r'%SystemDrive%\Users\John' RLIKE r'%SystemDrive%\\Users.*')
    and (r'%SystemDrive%\Users\John' RLIKE '%SystemDrive%\\\\Users.*')
    and (regexp_like('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*'))
    and (regexp_extract('100-200', '(\\d+)-(\\d+)', 1) > 10)
    and (regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1) != NULL)
    and (regexp_replace('100-200', '(\\d+)', 'num') != NULL)
    and (repeat('123', 2) != NULL)
    and (replace('ABCabc', 'abc', 'DEF') != NULL)
    and (reverse('Spark SQL') != NULL)
    and (RIGHT('Spark SQL', 3) != NULL)
    and (rpad('hi', 5, 'ab') != NULL)
    and (hex(rpad(x'1020', 5, x'05')) != NULL)
    and (rtrim('ab', 'SparkSQLabcaaba') != NULL)
    and (sentences('Hi there! Good morning.', 'en', 'US') != NULL)
    and (sha('Spark') != NULL)
    and (sha1('Spark') != NULL)
    and (sha2('Spark', 256) != NULL)
    and (soundex('Miller') != NULL)
    and (concat('1', space(2), '1') != NULL)
    and (('->' || split_part('Hello,world,!', ',', 1) || '<-') != NULL)
    and (('->' || split_part('', ',', 1) || '<-') != NULL)
    and (('->' || split_part('Hello,World,!', ',', 0) || '<-') != NULL)
    and (startswith('SparkSQL', 'Spark'))
    and (startswith(NULL, 'Spark'))
    and (substr('Spark SQL', 5, 1) != NULL)
    and (substring_index('www.apache.org', '.', 2) != NULL)
    and (CAST(to_binary('537061726B') AS STRING) != NULL)
    and (CAST(to_binary('537061726B', 'hex') AS STRING) != NULL)
    and (CAST(try_to_binary('U3Bhxcms=', 'base64') AS STRING) != NULL)
    and (translate('AaBbCc', 'abc', '123') != NULL)
    and (CAST(try_to_binary('U3Bhxcms=', 'base64') AS STRING) != NULL)
    and (ucase('SparkSql') != NULL)
    and (CAST(unbase64('U3BhcmsgU1FM') AS STRING) != NULL)
    and (decode(unhex('537061726B2053514C'), 'UTF-8') != NULL)
    and (upper('SparkSql') != NULL)
    and (10 / 2 == 5)
    and ((3 | 5) == 2)
    and ((DATE'2021-03-20' - INTERVAL '2' MONTH) != NULL)
    and (10 - 2 + 2 == 4)
    and (2 % 1.8 == 2)
    and (3 ^ 5 == 3)
    and (3 & 5 == 2)
    and (3 * 2 == 2)
    and ((INTERVAL '3' YEAR * 3) != NULL)
    and (abs(-1) == 1)
    and (acos(1) == 1)
    and (acosh(1) == 1)
    and (asin(0) == 1)
    and (asinh(0) == 1)
    and (atan(0) == 1)
    and (atan2(0, 0) == 1)
    and (atanh(0) == 1)
    and (bigint(current_timestamp) > 1)
    and (bit_count(-1) > 1)
    and (bit_get(23Y, 1) == 1)
    and (bround(13.5, -1) == 1)
    and (round(13.5, -1) == 1)
    and (cbrt(27.0) == 3)
    and (ceil(3345.1, -2) == 1)
    and (ceiling(5.4) == 6)
    and (ceiling(3345.1, -2) == 1)
    and (conv('100', 2, 10) == 4)
    and (conv('FFFFFFFFFFFFFFFF', 16, 10) == 1)
    and (cos(pi()) == -1)
    and (cosh(0) == 1)
    and (cot(1) == 1)
    and (csc(pi() / 2) == 2)
    and (decimal('5.2') == 5)
    and (degrees(3.141592653589793) == 10)
    and (double('5.2') == 4)
    and (e() == 2)
    and (exp(1) == 2)
    and (expm1(0) == 1)
    and (factorial(2) == 2)
    and (float('5.2') == 5)
    and (floor(-0.1) == -1)
    and (floor(3345.1, -2) == 20)
    and (getbit(23Y, 0) == 1)
    and (hypot(3, 4) == 5)
    and (int(-5.6) == 5)
    and (isnan(CAST('NaN' AS double)))
    and (ln(1) == 1)
    and (log(10, 100) == 2)
    and (log1p(0) == 1)
    and (log2(2) == 1)
    and (log10(10) == 1)
    and (nanvl(CAST('NaN' AS DOUBLE), 123) == 2)
    and (negative(1) == -1)
    and (pmod(-10, 3) == 2)
    and (positive(-1) == -1)
    and (pow(2, 3) * power(2, 3) == 8)
    and (radians(180) == 10)
    and (rand(0) * random(0) == 1)
    and (randn(0) == 1)
    and (rint(12.3456) == 1)
    and (round(2.5, 0) == 3)
    and (sec(pi()) == -1)
    and (sin(0) == 0)
    and (shiftleft(2, 1) == 2)
    and (shiftright(4, 1) == 2)
    and (shiftrightunsigned(4, 1) == 2)
    and (sign(40) == 1)
    and (signum(40) == 1)
    and (sinh(0) == 1)
    and (smallint(-5.6) == 5)
    and (sqrt(4) == 2)
    and (tan(0) == 1)
    and (tanh(0) == 1)
    and (tinyint('12') * tinyint(5.4) == 1)
    and (try_add(DATE'2021-03-20', INTERVAL '2' MONTH) != NULL)
    and (try_add(1, 2) == 3)
    and (try_divide(3, 2) == 3)
    and (try_divide(INTERVAL '3:15' HOUR TO MINUTE, 3) != NULL)
    and (try_subtract(1, 2) == 2)
    and (try_subtract(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL)
    and (try_subtract(-128Y, 1Y) != NULL)
    and (width_bucket(5.3, 0.2, 10.6, 5) + width_bucket(-0.9, 5.2, 0.5, 2) == 3)
    and (width_bucket(INTERVAL '1' DAY, INTERVAL '0' DAY, INTERVAL '10' DAY, 11) == 10)
    and (array(10, 20, 30) != NULL)
    and (
          (
            aggregate(array(1, 2, 3), 0, 
            (acc, x) -> acc + x, 
            acc -> acc * 10)
          ) == 1
        )
    and (array_contains(array(1, 2, 3), 2))
    and (array_distinct(array(1, 2, 3, NULL, 3)) != NULL)
    and (array_except(array(1, 2, 2, 3), array(1, 1, 3, 5)) != NULL)
    and (array_intersect(array(1, 2, 3), array(1, 3, 3, 5)) != NULL)
    and (array_join(array('hello', 'world'), ',') != NULL)
    and (array_join(array('hello', NULL, 'world'), ',', '*') == NULL)
    and (array_max(array(1, 20, NULL, 3)) > 10)
    and (array_min(array(1, 20, NULL, 3)) == 1)
    and (array_position(array(3, 2, 1, 4, 1), 1) == 2)
    and (array_remove(array(1, 2, 3, NULL, 3, 2), 3) != NULL)
    and (array_repeat('123', 2) != NULL)
    and (array_size(array(1, NULL, 3, NULL)) == 2)
    and (array_sort(array('bc', 'ab', 'dc')) != NULL)
    and (array_union(array(1, 2, 2, 3), array(1, 3, 5)) != NULL)
    and (arrays_overlap(array(1, 2, NULL, 3), array(NULL, 4, 5)) == NULL)
    and (arrays_zip(array(1, 2), array('shoe', 'string', 'budget')) == NULL)
    and (cardinality(array('b', 'd', 'c', 'a')) == 2)
    and (concat(array(1, 2, 3), array(4, 5), array(6)) != NULL)
    and (element_at(map(1, 'a', 2, 'b'), 2) == NULL)
    and (
          EXISTS (
            array(1, 2, 3),
            
            x -> x % 2 == 0
          )
        )
    and (
          EXISTS (
            array(0, NULL, 2, 3, NULL),
            
            x -> x IS NULL
          )
        )
    and (
          filter(array(1, 2, 3), 
          x -> x % 2 == 1) != NULL
        )
    and (flatten(array(array(1, 2), array(3, 4))) != NULL)
    and (
          forall(array(1, 2, 3), 
          x -> x % 2 == 0) == NULL
        )
    and (reverse(array(2, 1, 4, 3)) != NULL)
    and (sequence(5, 1) != NULL)
    and (cardinality(array('b', 'd', 'c', 'a')) == 10)
    and (slice(array(1, 2, 3, 4), 2, 2) != NULL)
    and (sort_array(array('b', 'd', NULL, 'c', 'a'), true) != NULL)
    and (
          transform(array(1, 2, 3), 
          x -> x + 1) != NULL
        )
    and (try_element_at(array(1, 2, 3), 2) == 2)
    and (
          zip_with(array('a', 'b', 'c'), array('d', 'e', 'f'), 
          (x, y) -> concat(x, y)) != NULL
        )
    and (map(1, 'Hello', 2, 'World')[1] != NULL)
    and (cardinality(map('a', 1, 'b', 2)) == 2)
    and (element_at(map(1, 'a', 2, 'b'), 3) == NULL)
    and (map(1.0, '2', 3.0, '4') IS NOT NULL)
    and (map_concat(map(1, 'a', 2, 'b'), map(3, 'c')) IS NOT NULL)
    and (map_contains_key(map(1, 'a', 2, 'b'), 2))
    and (map_entries(map(1, 'a', 2, 'b')) IS NOT NULL)
    and (
          map_filter(map(1, 0, 2, 2, 3, -1), 
          (k, v) -> k > v) IS NOT NULL
        )
    and (map_from_arrays(array(1.0, 3.0), array('2', '4')) IS NOT NULL)
    and (map_from_entries(array(struct(1, 'a'), struct(2, 'b'))) IS NOT NULL)
    and (map_keys(map(1, 'a', 2, 'b')) IS NOT NULL)
    and (map_values(map(1, 'a', 2, 'b')) IS NOT NULL)
    and (
          map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), 
          (k, v1, v2) -> concat(v1, v2)) IS NOT NULL
        )
    and (cardinality(map('a', 1, 'b', 2)) == 2)
    and (str_to_map('a:1,b:2,c:3', ',', ':') IS NOT NULL)
    and (
          transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
          (k, v) -> k + 1) IS NOT NULL
        )
    and (
          transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
          (k, v) -> k + v) IS NOT NULL
        )
    and (try_element_at(map(1, 'a', 2, 'b'), 2) IS NOT NULL)
    and ((INTERVAL '3:15' HOUR TO MINUTE / 3) IS NOT NULL)
    and ((DATE'2021-03-31' - INTERVAL '1' MONTH) != NULL)
    and (typeof(current_timestamp - (current_date + INTERVAL '1' DAY)) != NULL)
    and ((DATE'2021-03-31' + INTERVAL '1' MONTH) != NULL)
    and ((INTERVAL '3' YEAR * 3) != NULL)
    and (add_months('2016-08-31', -6) != NULL)
    and (current_date() != NULL)
    and (current_timestamp() != NULL)
    and (current_timezone() != NULL)
    and (date('2021-03-21') != NULL)
    and (date_add('2016-07-30', 1) != NULL)
    and (date_format('2016-04-08', 'y') == 2016)
    and (date_from_unix_date(1) != NULL)
    and (date_sub('2016-07-30', 1) != NULL)
    and (date_trunc('YEAR', '2015-03-05T09:32:05.359') != NULL)
    and (datediff('2009-07-31', '2009-07-30') == 1)
    and (day('2009-07-30') == 30)
    and (dayofmonth('2009-07-30') == 30)
    and (dayofweek('2009-07-30') == 5)
    and (dayofyear('2016-04-09') == 100)
    and (EXTRACT(SECONDS FROM INTERVAL '5:00:30.001' HOUR TO SECOND) == 30)
    and (EXTRACT(WEEK FROM TIMESTAMP'2019-08-12 01:00:00.123456') == 33)
    and (from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') != NULL)
    and (from_utc_timestamp('2017-07-14 02:40:00.0', 'GMT+1') != NULL)
    and (from_utc_timestamp('2016-08-31', 'Asia/Seoul') != NULL)
    and (hour('2009-07-30 12:58:59') == 10)
    and (last_day('2009-01-12') IS NOT NULL)
    and (make_date(2013, 7, 15) IS NOT NULL)
    and (make_dt_interval(0, 0, 1, -0.1) IS NOT NULL)
    and (make_ym_interval(100, 5) IS NOT NULL)
    and (minute('2009-07-30 12:58:59') == 58)
    and (month('2016-07-30') == 7)
    and (months_between('1997-02-28 10:30:00', '1996-10-30') == 3)
    and (months_between('1997-02-28 10:30:00', '1996-10-30', false) == 2)
    and (next_day('2015-01-14', 'TU') != NULL)
    and (now() != NULL)
    and (quarter('2016-08-31') == 3)
    and (second('2009-07-30 12:58:59') == 59)
    and (timestamp(123) != NULL)
    and (timestamp('2020-04-30 12:25:13.45') != NULL)
    and (timestamp_micros(1230219000123123) != NULL)
    and (timestamp_millis(1230219000123) != NULL)
    and (timestamp_seconds(1230219000) != NULL)
    and (to_date('2016-12-31', 'yyyy-MM-dd') != NULL)
    and (to_timestamp('2016-12-31', 'yyyy-MM-dd') != NULL)
    and (to_utc_timestamp('2017-07-14 02:40:00.0', 'GMT+1') != NULL)
    and (trunc('2015-10-27', 'YEAR') != NULL)
    and (trunc('2019-08-04', 'quarter') != NULL)
    and (try_add(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL)
    and (try_add(DATE'2021-03-31', INTERVAL '1' MONTH) != NULL)
    and (try_divide(INTERVAL '3:15' HOUR TO MINUTE, 3) != NULL)
    and ((INTERVAL '3' YEAR * 3) != NULL)
    and (try_subtract(DATE'2021-03-20', INTERVAL '2' MONTH) != NULL)
    and (try_subtract(TIMESTAMP'2021-03-20 12:15:29', INTERVAL '3' SECOND) != NULL)
    and (unix_date(DATE('1970-01-02')) == 1)
    and (unix_micros(TIMESTAMP('1970-01-01 00:00:01Z')) == 1)
    and (unix_millis(TIMESTAMP('1970-01-01 00:00:01Z')) == 1)
    and (unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z')) == 1)
    and (unix_timestamp('2016-04-08', 'yyyy-MM-dd') == 1)
    and (weekday(DATE'2009-07-30') == 1)
    and (EXTRACT(DAYOFWEEK_ISO FROM DATE'2009-07-30') == 4)
    and (weekofyear('2008-02-20') == 8)
    and (year('2016-07-30') == 2016)
    and (array(1, 2, 3) != NULL)
    and (bigint('5') == 5)
    and (binary('Spark SQL') != NULL)
    and (boolean(1))
    and (CAST(5.6 AS DECIMAL (2, 0)) != NULL)
    and (CAST(INTERVAL '1-2' YEAR TO MONTH AS INTEGER) == 12)
    and (date('2021-03-21') != NULL)
    and (decimal('5.2') == 5)
    and (double('5.2') / 2 == 5)
    and (float('5.2') / 2 == 2)
    and (int('5') == 5)
    and (make_date(2013, 7, 15) != NULL)
    and (make_dt_interval(100, 13) != NULL)
    and (make_ym_interval(100, 5) == NULL)
    and (map(1.0, '2', 3.0, '4') IS NOT NULL)
    and (named_struct('a', 1, 'b', 2, 'c', 3) IS NOT NULL)
    and (smallint('5') == 5)
    and (struct(1, 2, 3) IS NOT NULL)
    and (tinyint('12') == 12)
    and (timestamp('2020-04-30 12:25:13.45') != NULL)
    and (to_date('2016-12-31', 'yyyy-MM-dd') != NULL)
    and (to_timestamp('2016-12-31 00:12:00') != NULL)
    and (from_csv('1, 0.8', 'a INT, b DOUBLE') != NULL)
    and (schema_of_csv('1,abc') != NULL)
    and (json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') == 5)
    and (json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}') != NULL)
    and (schema_of_json('[{"col":01}]', map('allowNumericLeadingZeros', 'true')) != NULL)
    and (to_json(named_struct('a', 1, 'b', 2)) != NULL)
    and (to_json(map(named_struct('a', 1), named_struct('b', 2))) != NULL)
    and (to_json(array((map('a', 1)))) != NULL)
    and (xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()') != NULL)
    and (xpath_boolean('<a><b>1</b></a>', 'a/b'))
    and (xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') != NULL)
    and (xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)') != NULL)
    and (xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2)
    and (xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 3)
    and (xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2)
    and (xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') == 2)
    and (xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c') != NULL)
    and (
          assert_true(
            0 < 1) == NULL
        )
    and (
          (
            CASE
              WHEN 1 > 0
                THEN 1
              WHEN 2 > 0
                THEN 2.0
              ELSE 1.2
            END
          ) == 1
        )
    and (
          (
            CASE 3
              WHEN 1
                THEN 'A'
              WHEN 2
                THEN 'B'
              WHEN 3
                THEN 'C'
            END
          ) != NULL
        )
    and (coalesce(2, 5 / 0) == 2)
    and (current_catalog() != NULL)
    and (current_database() != NULL)
    and (current_user() != NULL)
    and (decode(5, 6, 'Spark', 5, 'SQL', 4, 'rocks') != NULL)
    and (elt(1, 'scala', 'java') != NULL)
    and (greatest(10, 9, 2, 4, 3) == 10)
    and (hash('Spark', array(123), 2) != NULL)
    and (
          if(
            1 < 2, 
            'a', 
            'b') != NULL
        )
    and (ifnull(NULL, array('2')) != NULL)
    and (input_file_block_length() == -1)
    and (input_file_block_start() == -1)
    and (isnull(1))
    and (isnotnull(1))
    and (least(10, 9, 2, 4, 3) == 2)
    and (monotonically_increasing_id() > 10)
    and (nullif(2, 2) == NULL)
    and (nvl(NULL, 2) == 2)
    and (nvl2(NULL, 2, 1) == 1)
    and (typeof(1) != NULL)
    and (uuid() != NULL)
    and (xxhash64('Spark', array(123), 2) != NULL)
    and ('20'::INTEGER == 20)
    and like('Spark', '_park')
    and ('Spark' LIKE SOME('_park', '_ock'))
    and (bitmap_count(x'00') == 10)
    and (charindex('bar', 'abcbarbar') == 1)
    and (decode(x'FEFF0053007000610072006B002000530051004C', 'UTF-16') != NULL)
    and (like('Spark', '_park'))
    and (len('Spark SQL ') > 10)
    and (levenshtein('kitten', 'sitting', 4) > 10)
    and (('+' || ltrim('abc', 'acbabSparkSQL   ') || '+') != NULL)
    and (mask('AaBb123-&^ASDXYZ921312asd', 'Z', 'z', '9', 'X') != NULL)
    and (mask('AaBb123-&^ASDXYZ921312asd', lowerChar => 'z', upperChar => 'X') != NULL)
    and (mask('AaBb123-&ASDXYZ921312asd', NULL, NULL, NULL, NULL) != NULL)
    and (overlay('Spark SQL' PLACING '_' FROM 6) != NULL)
    and (overlay('Spark SQL' PLACING 'tructured' FROM 2 FOR 4) != NULL)
    and (overlay(encode('Spark SQL', 'utf-8') PLACING encode('_', 'utf-8') FROM 6) != NULL)
    and (position('bar' IN 'abcbarbar') > 2)
    and (regexp_count('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en') > 2)
    and (regexp_instr('Mary had a little lamb', NULL) != NULL)
    and (regexp_substr(NULL, 'Ste(v|ph)en') != NULL)
    and (rtrim('ab', 'SparkSQLabcaaba') != NULL)
    and (string(4) != NULL)
    and (substr('Spark SQL', -3) != NULL)
    and (substr('Spark SQL' FROM 5 FOR 1) != NULL)
    and (substr('Spark SQL' FROM -3) != NULL)
    and (to_char(454, '000.00') != NULL)
    and (to_char(DATE'2016-04-08', 'y') != NULL)
    and (to_char(encode('abc', 'utf-8'), 'utf-8') != NULL)
    and (to_varchar(454, '999') != NULL)
    and (to_varchar(DATE'2016-04-08', 'y') != NULL)
    and (to_varchar(x'537061726b2053514c', 'hex') != NULL)
    and (TRIM( 'SL' FROM 'SSparkSQLS') != NULL)
    and (TRIM(BOTH 'SL' FROM 'SSparkSQLS') != NULL)
    and (TRIM(LEADING 'SL' FROM 'SSparkSQLS') != NULL)
    and (TRIM(TRAILING 'SL' FROM 'SSparkSQLS') != NULL)
    and (
          CAST(try_aes_decrypt(
            unbase64('MTIzNDU2Nzg5MDEyMdXvR41sJqwZ6hnTU8FRTTtXbL8yeChIZA=='), 
            '1234567890abcdef', 
            'GCM', 
            'DEFAULT', 
            'Some AAD') AS STRING) != NULL
        )
    and (url_decode('http%3A%2F%2Fspark.apache.org%2Fpath%3Fquery%3D1') != NULL)
    and (url_encode('http://spark.apache.org/path?query=1') != NULL)
    and ((TIMESTAMP'2021-03-20 12:15:29' - INTERVAL '3' SECOND) != NULL)
    and ((TIMESTAMP'2021-03-20 12:15:29' + INTERVAL '3' SECOND) != NULL)
    and (bit_reverse(-1) == -1)
    and (bitmap_bit_position(-32768) == 1)
    and (bitmap_bucket_number(-32768) == 1)
    and (MOD(2, 1.8) == 2)
    and (array_append(array(1, 2, 3), 0) == NULL)
    and (array_compact(array(1, 2, NULL, 3, NULL, 3)) == NULL)
    and (array_insert(array('a', 'b', 'c'), 1, 'z') != NULL)
    and (array_prepend(array(1, 2, 3), 0) != NULL)
    and (get(array(1, 2, 3), 2) != NULL)
    and (
          reduce(array(1, 2, 3), 0, 
          (acc, x) -> acc + x) == 2
        )
    and (shuffle(array(1, 20, 3, 5)) != NULL)
    and (map_contains_key(map(1, 'a', 2, 'b'), 2))
    and ((DATE'2021-03-31' - INTERVAL '1' MONTH) != NULL)
    and ((DATE'2021-03-31' + INTERVAL '1' MONTH) != NULL)
    and ((TIMESTAMP'2021-03-20 12:15:29' - INTERVAL '3' SECOND) != NULL)
    and (date_diff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1)
    and (date_part('SECONDS', TIMESTAMP'2019-10-01 00:00:01.000001') == 1)
    and (date_part('Week', TIMESTAMP'2019-08-12 01:00:00.123456') == 33)
    and (dateadd('2016-07-30', 1) != NULL)
    and (dateadd(MICROSECOND, 5, TIMESTAMP'2022-02-28 00:00:00') != NULL)
    and (datediff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59') == 0)
    and (make_interval(0, 0, 1, 1, 12, 30, 1.001001) IS NOT NULL)
    and (make_timestamp(2014, 12, 28, 6, 30, 45.887, 'CET') IS NOT NULL)
    and (make_timestamp(NULL, 7, 22, 15, 30, 0) IS NOT NULL)
    and (now() != NULL)
    and (timediff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1)
    and (timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00') == 1)
    and (to_unix_timestamp('2016-04-08', 'yyyy-MM-dd') == 100)
    and (try_to_timestamp('2016-12-31', 'yyyy-MM-dd') != NULL)
    and ('20'::INTEGER == 20)
    and (make_interval(100, 11) IS NOT NULL)
    and (make_timestamp(2014, 12, 28, 6, 30, 45.887) IS NOT NULL)
    and (string(5) != NULL)
    and (to_char(454, '000.00') != NULL)
    and (to_varchar(454, '999') IS NOT NULL)
    and (from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE') IS NOT NULL)
    and (get_json_object('{"a":"b"}', '$.a') IS NOT NULL)
    and (to_csv(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy')) IS NOT NULL)
    and (to_csv(named_struct('a', 1, 'b', 2)) IS NOT NULL)
    and (from_xml('<p><time>26/08/2015</time></p>', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy')) != NULL)
    and (schema_of_xml('<p><a attr="2">1</a><a>3</a></p>', map('excludeAttribute', 'true')) IS NOT NULL)
    and (current_metastore() != NULL)
    and (current_schema() != NULL)
    and (current_version() != NULL)
    and (equal_null(2, 2))
    and (
          iff(
            1 < 2, 
            'a', 
            'b') == 'a'
        )
    and (is_account_group_member('admins'))
    and (is_member('admins'))
    and (luhn_check('12345') == NULL)
    and (user() != NULL)
    and (h3_coverash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 0) != NULL)
    and (h3_coverash3string('{"type":"LineString","coordinates":[[-122.4194,37.7749],[-118.2437,34.0522],[-74.0060,40.7128]]}', 1) != NULL)
    and (h3_longlatash3(-122.4783, 37.8199, 13) > 0)
    and (h3_longlatash3string(-122.4783, 37.8199, 13) != NULL)
    and (h3_pointash3('POINT(-122.4783 37.8199)', 13) > 0)
    and (h3_pointash3string('{"type":"Point","coordinates":[]}', 15) == NULL)
    and (h3_pointash3string('POINT(-122.4783 37.8199)', 13) != NULL)
    and (
          h3_polyfillash3(
            unhex(
              '0103000000010000000400000050fc1873d79a5ec0d0d556ec2fe342404182e2c7988f5dc0f46c567dae064140aaf1d24d628052c05e4bc8073d5b444050fc1873d79a5ec0d0d556ec2fe34240'), 
            2) != NULL
        )
    and (h3_polyfillash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL)
    and (h3_polyfillash3string('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL)
    and (h3_try_polyfillash3('Not-a-valid-rep', 2) == NULL)
    and (h3_try_polyfillash3('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL)
    and (h3_try_polyfillash3string('POLYGON((-122.4194 37.7749,-118.2437 34.0522,-74.0060 40.7128,-122.4194 37.7749))', 2) != NULL)
    and (h3_boundaryasgeojson('8009fffffffffff') != NULL)
    and (h3_boundaryasgeojson(599686042433355775) != NULL)
    and (hex(h3_boundaryaswkb(599686042433355775)) != NULL)
    and (h3_boundaryaswkt(599686042433355775) != NULL)
    and (h3_centerasgeojson(599686042433355775) != NULL)
    and (hex(h3_centeraswkb('8009fffffffffff')) == NULL)
    and (h3_centeraswkt('8009fffffffffff') != NULL)
    and (h3_h3tostring(599686042433355775) != NULL)
    and (h3_stringtoh3('85283473fffffff') == NULL)
    and (h3_ischildof('88283471b9fffff', '85283473fffffff'))
    and (h3_ispentagon(590112357393367039))
    and (h3_isvalid('85283473fffffff'))
    and (h3_try_validate(590112357393367039) != NULL)
    and (h3_validate(590112357393367039) != NULL)
    and (h3_distance(599686030622195711, 599686015589810175) == 2)
    and (h3_hexring(599686042433355775, 1) != NULL)
    and (h3_kring(599686042433355775, 1) != NULL)
    and (h3_kringdistances(599686042433355775, 1) != NULL)
    and (h3_distance(599686030622195711, 599686015589810175) == 2)
    and (h3_maxchild(599686042433355775, 10) != NULL)
    and (h3_minchild(599686042433355775, 10) != NULL)
    and (h3_resolution(599686042433355775) == 5)
    and (h3_tochildren(599686042433355775, 6) != NULL)
    and (h3_toparent(599686042433355775, 0) != NULL)
    and (
          h3_compact(
            ARRAY(
              599686042433355775, 
              599686030622195711, 
              599686044580839423, 
              599686038138388479, 
              599686043507097599, 
              599686015589810175, 
              599686014516068351, 
              599686034917163007, 
              599686029548453887, 
              599686032769679359, 
              599686198125920255, 
              599686040285872127, 
              599686041359613951, 
              599686039212130303, 
              599686023106002943, 
              599686027400970239, 
              599686013442326527, 
              599686012368584703, 
              599686018811035647)) != NULL
        )
    and (
          h3_uncompact(
            ARRAY(
              599686030622195711, 
              599686015589810175, 
              599686014516068351, 
              599686034917163007, 
              599686029548453887, 
              599686032769679359, 
              599686198125920255, 
              599686023106002943, 
              599686027400970239, 
              599686013442326527, 
              599686012368584703, 
              599686018811035647, 
              595182446027210751), 
            5) != NULL
        ) AS c_expressions
  
  FROM Reformat_1_1 AS in0

),

env_uitesting_shared_parent_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_parent_model_1')}}

),

AllStunningOne AS (

  SELECT 
    (1 != 2)
    or (true != NULL)
    or (NULL != NULL)
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
    or (array_join(array('hello', NULL, 'world'), ' ', ',') LIKE '%hello%')
    or (array_max(array(1, 20, NULL, 3)) > 10)
    or (array_min(array(1, 20, NULL, 3)) > 20)
    or array_contains(array_remove(array(1, 2, 3, NULL, 3), 3), 3)
    or array_contains(array_repeat(5, 2), 6)
    or array_contains(array_union(array(1, 2, 3), array(1, 3, 5)), 10)
    or arrays_overlap(array(1, 2, 3), array(3, 4, 5))
    or (10 BETWEEN 2 and 20)
    or contains('Spark SQL', 'Spark')
    or endswith('Spark SQL', 'SQL')
    or (
         EXISTS (
           array(1, 2, 3),
           
           x -> x % 2 == 0
         )
       )
    or array_contains(filter(array(1, 2, 3), 
       x -> x % 2 == 1), 5)
    or array_contains(flatten(array(array(1, 2), array(3, 4))), 10)
    or forall(array(1, 2, 3), 
       x -> x % 2 == 0)
    or ilike('Spark', '_Park')
    or (1 IN (2, 3, 4))
    or (isnan(CAST('NaN' AS double)))
    or isnotnull(1)
    or isnull(1)
    or array_contains(json_object_keys('{"key": "value"}'), 'key1')
    or like('Spark', '_park')
    or map_contains_key(map(1, 'a', 2, 'b'), 1)
    or map_contains_key(map_concat(map(1, 'a', 2, 'b'), map(3, 'c')), 4)
    or map_contains_key(map_filter(map(1, 0, 2, 2, 3, -1), 
       (k, v) -> k > v), 3)
    or map_contains_key(map_from_arrays(array(1.0, 3.0), array('2', '4')), 2)
    or map_contains_key(map_from_entries(array(struct(1, 'a'), struct(2, 'b'))), 1)
    or array_contains(map_keys(map(1, 'a', 2, 'b')), 2)
    or array_contains(map_values(map(1, 'a', 2, 'b')), 'a')
    or map_contains_key(map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), 
       (k, v1, v2) -> concat(v1, v2)), 1)
    or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
    or (NOT true)
    or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100')
    or array_contains(sequence(1, 5), 4)
    or array_contains(shuffle(array(1, 20, 3, 5)), 10)
    or array_contains(slice(array(1, 2, 3, 4), 2, 2), 4)
    or array_contains(sort_array(array('b', 'd', NULL, 'c', 'a'), true), 'b')
    or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one')
    or startswith('Spark SQL', 'Spark')
    or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a')
    or array_contains(transform(array(1, 2, 3), 
       x -> x + 1), 1)
    or map_contains_key(transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
       (k, v) -> k + 1), 1)
    or map_contains_key(transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
       (k, v) -> v + 1), 2)
    or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a')
    or xpath_boolean('<a><b>1</b></a>', 'a/b')
    or array_contains(zip_with(array(1, 2), array(3, 4), 
       (x, y) -> x + y), 1) AS c_bool_expr,
    concat(
      c_array[0], 
      c_struct.city, 
      aes_decrypt(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333'), 
      base64(aes_encrypt('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS')), 
      bin(13), 
      btrim('    SparkSQL   '), 
      char(65), 
      chr(65), 
      concat('Spark', 'SQL'), 
      concat_ws(' ', 'Spark', 'SQL'), 
      crc32('Spark'), 
      current_catalog(), 
      current_database(), 
      current_date(), 
      current_timestamp(), 
      current_timezone(), 
      current_user(), 
      date_add('2016-07-30', 1), 
      date_sub('2016-07-30', 1), 
      date_format('2016-04-08', 'y'), 
      date_from_unix_date(1), 
      date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'), 
      date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH), 
      date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND), 
      date_trunc('HOUR', '2015-03-05T09:32:05.359'), 
      date_trunc('DD', '2015-03-05T09:32:05.359'), 
      datediff('2009-07-31', '2009-07-30'), 
      decode(encode('abc', 'utf-8'), 'utf-8'), 
      e(), 
      elt(1, 'scala', 'java'), 
      format_number(12332.123456, '##################.###'), 
      format_string('Hello World %d %s', 100, 'days'), 
      CAST(from_csv('1, 0.8', 'a INT, b DOUBLE') AS string), 
      CAST(from_json(
        '{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 
        'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS string), 
      CAST(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS string), 
      CAST(from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS string), 
      CAST(get_json_object('{"a":"b"}', '$.a') AS string), 
      hash('Spark', array(123), 2), 
      hex(17), 
      CAST(hour('2009-07-30 12:58:59') AS string), 
      CAST(hypot(3, 4) AS string), 
      CAST(ilike('Spark', '_Park') AS string), 
      CAST(initcap('sPark sql') AS string), 
      CAST(last_day('2009-01-12') AS string), 
      CAST(lcase('SparkSql') AS string), 
      CAST(if(
        1 < 2, 
        'a', 
        'b') AS string), 
      CAST(ifnull(NULL, array('2')) AS string), 
      LEFT('Spark SQL', 3), 
      lower('SparkSql'), 
      lpad('hi', 5, '??'), 
      ltrim('    SparkSQL   '), 
      CAST(make_date(2013, 7, 15) AS string), 
      CAST(make_dt_interval(1, 12, 30, 1.001001) AS string), 
      CAST(make_interval(100, 11, 1, 1, 12, 30, 1.001001) AS string), 
      CAST(make_timestamp(2019, 6, 30, 23, 59, 60) AS string), 
      CAST(make_ym_interval(1, 2) AS string), 
      md5('Spark'), 
      next_day('2015-01-14', 'TU'), 
      now(), 
      nullif(2, 2), 
      nvl(NULL, 'hello'), 
      CAST(overlay('Spark SQL' PLACING '_' FROM 6) AS string), 
      CAST(parse_url('http://spark.apache.org/path?query=1', 'HOST') AS string), 
      printf('Hello World %d %s', 100, 'days'), 
      CAST(regexp_extract('100-200', '(\\d+)-(\\d+)', 1) AS string), 
      CAST(regexp_replace('100-200', '(\\d+)', 'num') AS string), 
      repeat('123', 2), 
      replace('ABCabc', 'abc', 'DEF'), 
      reverse('Spark SQL'), 
      RIGHT('Spark SQL', 3), 
      rpad('hi', 5, '??'), 
      rtrim('    SparkSQL   '), 
      CAST(schema_of_json('[{"col":0}]') AS string), 
      sha('Spark'), 
      sha1('Spark'), 
      sha2('Spark', 256), 
      concat(space(2), '1'), 
      split_part('11.12.13', '.', 3), 
      substr('Spark SQL', 5), 
      substring('Spark SQL', 5), 
      substring_index('www.apache.org', '.', 2), 
      timestamp_micros(1230219000123123), 
      timestamp_millis(1230219000123), 
      timestamp_seconds(1.230219000123E9), 
      to_csv(named_struct('a', 1, 'b', 2)), 
      to_date('2009-07-30 04:17:52'), 
      to_timestamp('2016-12-31 00:12:00'), 
      to_unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
      to_utc_timestamp('2016-08-31', 'Asia/Seoul'), 
      translate('AaBbCc', 'abc', '123'), 
      trunc('2019-08-04', 'week'), 
      try_to_binary('abc', 'utf-8'), 
      try_to_number('454', '999'), 
      typeof(1), 
      ucase('SparkSql'), 
      unbase64('U3BhcmsgU1FM'), 
      decode(unhex('537061726B2053514C'), 'UTF-8'), 
      unix_date(DATE("1970-01-02")), 
      unix_micros(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_millis(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z')), 
      unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
      upper('SparkSql'), 
      uuid(), 
      xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c'), 
      xxhash64('Spark', array(123), 2), 
      YEAR ('2016-07-30'), 
      to_json(array(named_struct('a', 1, 'b', 2)))) AS c_concat_expr,
    (2 % 1.8)
    + '20'::INTEGER
    + (MOD(2, 1.8))
    + (3 & 5)
    + (2 * 3)
    + (5 + 10)
    - (100 + 45)
    + (3 / 2)
    + (3 ^ 5)
    + abs(-1)
    + acos(1)
    + acosh(1)
    + array_position(array(3, 2, 1), 1)
    + array_size(array('b', 'd', 'c', 'a'))
    + ascii(2)
    + asin(0)
    + asinh(0)
    + atan(0)
    + atan2(0, 0)
    + atanh(0)
    + bit_count(0)
    + bit_get(11, 0)
    + bit_length('Spark SQL')
    + bround(25, -1)
    + cardinality(array('b', 'd', 'c', 'a'))
    + cardinality(map('a', 1, 'b', 2))
    + CAST('10' AS int)
    + cbrt(27.0)
    + ceil(3.1411, 3)
    + ceiling(3.1411, 3)
    + char_length('Spark SQL ')
    + coalesce(NULL, 1, NULL)
    + conv('100', 2, 10)
    + cos(0)
    + cosh(0)
    + cot(1)
    + csc(1)
    + day('2009-07-30')
    + dayofmonth('2009-07-30')
    + dayofweek('2009-07-30')
    + dayofyear('2016-04-09')
    + degrees(3.141592653589793)
    + element_at(array(1, 2, 3), 2)
    + exp(0)
    + expm1(0)
    + EXTRACT(SECONDS FROM TIMESTAMP'2019-10-01 00:00:01.000001')
    + EXTRACT(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND)
    + factorial(2)
    + find_in_set('ab', 'abc,b,ab,c,def')
    + floor(-0.1)
    + getbit(11, 0)
    + greatest(10, 9, 2, 4, 3)
    + instr('SparkSQL', 'SQL')
    + json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]')
    + least(10, 9, 2, 4, 3)
    + length('Spark SQL ')
    + levenshtein('kitten', 'sitting')
    + ln(10)
    + locate('bar', 'foobarbar')
    + log(10, 100)
    + log10(10)
    + log1p(0)
    + log2(2)
    + minute('2009-07-30 12:58:59')
    + (2 % 1.8)
    + month('2016-07-30')
    + months_between('1997-02-28 10:30:00', '1996-10-30', false)
    + nanvl(CAST('NaN' AS double), 123)
    + negative(100)
    + nvl2(NULL, 2, 1)
    + octet_length('Spark SQL')
    + pi()
    + pmod(10, 3)
    + position('bar', 'foobarbar')
    + positive(1)
    + pow(2, 3)
    + power(2, 3)
    + quarter('2016-08-31')
    + radians(180)
    + rand()
    + randn()
    + random()
    + rint(12.3456)
    + round(2.5, 0)
    + sec(0)
    + second('2009-07-30 12:58:59')
    + shiftleft(2, 1)
    + shiftright(4, 1)
    + shiftrightunsigned(4, 1)
    + sign(40)
    + signum(40)
    + sin(0)
    + sinh(0)
    + size(array('b', 'd', 'c', 'a'))
    + sqrt(4)
    + tan(0)
    + tanh(0)
    + to_number('454.00', '000.00')
    + try_add(1, 2)
    + try_divide(2L, 2L)
    + try_element_at(array(1, 2, 3), 2)
    + try_multiply(2, 3)
    + try_subtract(2, 1)
    + weekday('2009-07-30')
    + weekofyear('2008-02-20')
    + (
        CASE
          WHEN 1 > 0
            THEN 1
          WHEN 2 > 0
            THEN 2.0
          ELSE 1.2
        END
      )
    + width_bucket(5.3, 0.2, 10.6, 5)
    + xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
    + (~ 0) AS `c_add_expr`,
    c_tinyint AS c_tinyint,
    c_smallint AS `c_smallint`,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_float AS c_float,
    c_double AS c_double,
    c_string AS c_string,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_struct AS c_struct,
    {{ SQL_DatabricksSharedBasic.qa_concat_function_main('c_string', 'c_boolean') }} AS c_macro,
    {% if v_int > 20 %}
      concat(c_string, c_float) AS c_if,
    {% elif  var('v_project_dict')['b'] == 'hello' %}
      concat(c_string, c_int) AS c_if,
    {% else %}
      concat(c_string, c_bigint) AS c_if,
    {% endif %}
    {% for c_i in range(0, 5) %}
      concat(c_string, {{c_i}}) AS cfor_col_{{c_i}},
    {% endfor %}
    
    {{ SQL_DatabricksParentProjectMain.qa_boolean_macro('c_string') }} AS c_databricks_project_main,
    {{ SQL_BaseGitDepProjectAllFinal.qa_concat_macro_base_column('c_string') }} AS c_base_project,
    concat('{{ dbt_utils.pretty_time() }}', '{{ dbt_utils.pretty_log_format("my pretty message") }}') AS c_dbt_utils_functions,
    {{v_expression}} AS c_use_config_expression,
    area(c_int, c_int) AS c_use_databricks_function,
    to_json(named_struct('a', 1, 'b', 2)) AS c_to_json
  
  FROM env_uitesting_shared_parent_model_1 AS in0

),

Aggregate_1 AS (

  SELECT 
    any_value(c_bool_expr) AS c_bool_expr,
    any_value(c_concat_expr) AS c_concat_expr,
    any_value(c_add_expr) AS c_add_expr,
    any_value(c_tinyint) AS c_tinyint,
    any_value(c_smallint) AS c_smallint,
    any_value(c_int) AS c_int,
    any_value(c_bigint) AS c_bigint,
    any_value(c_float) AS c_float,
    any_value(c_double) AS c_double,
    any_value(c_string) AS c_string,
    any_value(c_boolean) AS c_boolean,
    any_value(c_macro) AS c_macro,
    any_value(c_if) AS c_if,
    any_value(cfor_col_0) AS cfor_col_0,
    any_value(cfor_col_1) AS cfor_col_1,
    any_value(cfor_col_2) AS cfor_col_2,
    any_value(cfor_col_3) AS cfor_col_3,
    any_value(cfor_col_4) AS cfor_col_4,
    any_value(c_databricks_project_main) AS c_databricks_project_main,
    any_value(c_base_project) AS c_base_project,
    any_value(c_dbt_utils_functions) AS c_dbt_utils_functions
  
  FROM AllStunningOne AS in0
  
  GROUP BY c_boolean

),

store_sales AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'store_sales') }}

),

SQLStatement_3 AS (

  SELECT *
  
  FROM (
    SELECT count(*) AS h8_30_to_9
    
    FROM spark_catalog.qa_database.store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 8
          and time_dim.t_minute >= 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s1, (
    SELECT count(*) AS h9_to_9_30
    
    FROM spark_catalog.qa_database.store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 9
          and time_dim.t_minute < 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s2, (
    SELECT count(*) AS h9_30_to_10
    
    FROM spark_catalog.qa_database.store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 9
          and time_dim.t_minute >= 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s3, (
    SELECT count(*) AS h10_to_10_30
    
    FROM store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 10
          and time_dim.t_minute < 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s4, (
    SELECT count(*) AS h10_30_to_11
    
    FROM spark_catalog.qa_database.store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 10
          and time_dim.t_minute >= 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s5, (
    SELECT count(*) AS h11_to_11_30
    
    FROM spark_catalog.qa_database.store_sales, spark_catalog.qa_database.household_demographics, spark_catalog.qa_database.time_dim, spark_catalog.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 11
          and time_dim.t_minute < 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s6, (
    SELECT count(*) AS h11_30_to_12
    
    FROM hive_metastore.qa_database.store_sales, hive_metastore.qa_database.household_demographics, hive_metastore.qa_database.time_dim, hive_metastore.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 11
          and time_dim.t_minute >= 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s7, (
    SELECT count(*) AS h12_to_12_30
    
    FROM hive_metastore.qa_database.store_sales, hive_metastore.qa_database.household_demographics, hive_metastore.qa_database.time_dim, hive_metastore.qa_database.store
    
    WHERE ss_sold_time_sk = time_dim.t_time_sk
          and ss_hdemo_sk = household_demographics.hd_demo_sk
          and ss_store_sk = s_store_sk
          and time_dim.t_hour = 12
          and time_dim.t_minute < 30
          and (
                (household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count <= 0 + 2)
                or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count <= 1 + 2)
                or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count <= -1 + 2)
              )
          and store.s_store_name = 'ese'
  ) AS s8

),

raw_customers AS (

  SELECT * 
  
  FROM {{ ref('raw_customers')}}

),

qa_complex_macro_1 AS (

  {{ SQL_DatabricksParentProjectMain.qa_complex_macro(model = 'raw_customers', column_name_int = 'id', accepted_values = [
    1, 
    2, 
    3, 
    4, 
    5
  ]) }}

),

Reformat_1 AS (

  SELECT 
    'This is my first name' AS first_name,
    'This is my last name' AS last_name,
    1 + col_int AS id
  
  FROM qa_complex_macro_1 AS in0

),

Filter_1 AS (

  SELECT * 
  
  FROM Reformat_1 AS in0
  
  WHERE true

),

item AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'item') }}

),

SQLStatement_1_1 AS (

  SELECT 
    substr(w_warehouse_name, 1, 20),
    sm_type,
    cc_name,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30)
        THEN 1
      ELSE 0
    END) AS days_30,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) and (cs_ship_date_sk - cs_sold_date_sk <= 60)
          THEN 1
        ELSE 0
      END) AS days_31_60,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) and (cs_ship_date_sk - cs_sold_date_sk <= 90)
          THEN 1
        ELSE 0
      END) AS days_61_90,
    sum(
      CASE
        WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) and (cs_ship_date_sk - cs_sold_date_sk <= 120)
          THEN 1
        ELSE 0
      END) AS days_90_120,
    sum(CASE
      WHEN (cs_ship_date_sk - cs_sold_date_sk > 120)
        THEN 1
      ELSE 0
    END) AS days_more_than_120
  
  FROM hive_metastore.qa_database.catalog_sales, hive_metastore.qa_database.warehouse, hive_metastore.qa_database.ship_mode, hive_metastore.qa_database.call_center, hive_metastore.qa_database.date_dim
  
  WHERE d_month_seq BETWEEN 1200
        and 1200 + 11
        and cs_ship_date_sk = d_date_sk
        and cs_warehouse_sk = w_warehouse_sk
        and cs_ship_mode_sk = sm_ship_mode_sk
        and cs_call_center_sk = cc_call_center_sk
  
  GROUP BY 
    substr(w_warehouse_name, 1, 20), sm_type, cc_name
  
  ORDER BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
  
  LIMIT 100

),

date_dim AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'date_dim') }}

),

SQLStatement_2 AS (

  SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    sum(ss_ext_sales_price) AS itemrevenue,
    sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
  
  FROM store_sales, item, date_dim
  
  WHERE ss_item_sk = i_item_sk
        and i_category IN ('Women', 'Electronics', 'Shoes')
        and ss_sold_date_sk = d_date_sk
        and d_date BETWEEN CAST('2002-05-27' AS date)
        and dateadd(DAY, 30, to_date('2002-05-27'))
  
  GROUP BY 
    i_item_id, i_item_desc, i_category, i_class, i_current_price
  
  ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio

),

Join_1_1 AS (

  SELECT 
    in0.`substr(w_warehouse_name, 1, 20)` AS substrw_warehouse_name120,
    in0.sm_type AS sm_type,
    in0.cc_name AS cc_name,
    in0.days_30 AS days_30,
    in0.days_31_60 AS days_31_60,
    in0.days_61_90 AS days_61_90,
    in0.days_90_120 AS days_90_120,
    in0.days_more_than_120 AS days_more_than_120,
    in1.i_item_id AS i_item_id,
    in2.h8_30_to_9 AS h8_30_to_9
  
  FROM SQLStatement_1_1 AS in0
  INNER JOIN SQLStatement_2 AS in1
     ON in0.cc_name != in1.i_item_desc
  INNER JOIN SQLStatement_3 AS in2
     ON in1.i_current_price != in2.h8_30_to_9

),

env_uitesting_shared_child_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_child_model_1')}}

),

Limit_6 AS (

  SELECT * 
  
  FROM env_uitesting_shared_child_model_1 AS in0
  
  LIMIT 10

),

SQLStatement_1_2 AS (

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
  
  UNION
  
  SELECT CAST(any_value(col) AS string) AS c1
  
  FROM VALUES
        (10),
        (5),
        (20) AS tab(col)
  
  UNION
  
  SELECT CAST(approx_top_k(expr) AS string) AS c1
  
  FROM VALUES
        (0),
        (0),
        (1),
        (1),
        (2),
        (3),
        (4),
        (4) AS tab(expr)
  
  UNION
  
  SELECT CAST(hll_sketch_estimate(hll_sketch_agg(col, 12)) AS string) AS c1
  
  FROM VALUES
        (1),
        (1),
        (2),
        (2),
        (3) AS tab(col)
  
  UNION
  
  -- SELECT CAST(a AS string) AS c1
  -- FROM (
  --   SELECT hll_sketch_estimate(hll_union(hll_sketch_agg(col1, 4), hll_sketch_agg(col2, 21))) AS a
  --   FROM VALUES
  --         (1, 4),
  --         (1, 4),
  --         (2, 5),
  --         (2, 5),
  --         (3, 6) AS tab(col1, col2)
  -- )
  -- UNION
  SELECT CAST(median(col) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (2),
        (3),
        (4),
        (NULL) AS tab(col)
  
  UNION
  
  SELECT CAST(regr_intercept(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(regr_slope(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(typeof(regr_sxx(y, x)) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(regr_sxy(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(regr_syy(y, x) AS string) AS c1
  
  FROM VALUES
        (1, 2),
        (2, 3),
        (2, 3),
        (NULL, 4),
        (4, NULL) AS T(y, x)
  
  UNION
  
  SELECT CAST(mode(col) AS string) AS c1
  
  FROM VALUES
        (array(1, 2)),
        (array(1, 2)),
        (array(2, 3)) AS tab(col)
  
  UNION
  
  SELECT CAST(approx_top_k(expr, 10, 100) AS string) AS c1
  
  FROM VALUES
        (0),
        (1),
        (1),
        (2),
        (2),
        (2) AS tab(expr)
  
  UNION
  
  SELECT CAST(hll_sketch_estimate(hll_sketch_agg(col, 12)) AS string) AS c1
  
  FROM VALUES
        (1),
        (1),
        (2),
        (2),
        (3) AS tab(col)
  
  UNION
  
  SELECT CAST(hll_sketch_estimate(hll_union_agg(sketch, true)) AS string) AS c1
  
  FROM (
    SELECT hll_sketch_agg(col) AS sketch
    
    FROM VALUES
          (1) AS tab(col)
    
    UNION ALL
    
    SELECT hll_sketch_agg(col, 20) AS sketch
    
    FROM VALUES
          (1) AS tab(col)
  )
  
  UNION
  
  SELECT CAST(hex(TRIM(TRAILING x'00' FROM bitmap_construct_agg(val))) AS string) AS c1
  
  FROM VALUES
        (0) AS T(val)
  
  UNION
  
  SELECT CAST(num_distinct AS string) AS c1
  
  FROM (
    SELECT sum(num_distinct) AS num_distinct
    
    FROM (
      SELECT 
        bitmap_bucket_number(val),
        bitmap_count(bitmap_construct_agg(bitmap_bit_position(val)))
      
      FROM VALUES
            (1),
            (2),
            (1),
            (-1),
            (5),
            (0),
            (5) AS t(val)
      
      GROUP BY ALL  
    ) AS distinct_vals_by_bucket(bucket, num_distinct)
  )
  
  UNION
  
  SELECT CAST(hex(count_min_sketch(column => col, confidence => 0.5d, epsilon => 0.5d, seed => 1)) AS string) AS c1
  
  FROM VALUES
        (1),
        (2),
        (1) AS tab(col)
  
  UNION
  
  SELECT CAST(num_distinct AS string) AS c1
  
  FROM (
    SELECT sum(num_distinct) AS num_distinct
    
    FROM (
      SELECT 
        bucket,
        bitmap_count(bitmap_or_agg(num_distinct)) AS num_distinct
      
      FROM (
        (SELECT 
          bitmap_bucket_number(val) AS bucket,
          bitmap_construct_agg(bitmap_bit_position(val)) AS num_distinct
        
        FROM VALUES
              (1),
              (2),
              (1),
              (-1),
              (5),
              (0),
              (5) AS t(val)
        
        GROUP BY ALL)
        
        UNION ALL
        
        (SELECT 
          bitmap_bucket_number(val) AS bucket,
          bitmap_construct_agg(bitmap_bit_position(val)) AS num_distinct
        
        FROM VALUES
              (3),
              (1),
              (-1),
              (6),
              (5),
              (1),
              (5),
              (8) AS t(val)
        
        GROUP BY ALL)
      )
      
      GROUP BY ALL  
    )
  )

),

Join_1_2 AS (

  SELECT 
    in0.customer_id AS customer_id,
    in0.first_name AS first_name,
    in0.last_name AS last_name,
    in0.c_expressions AS c_expressions,
    in1.c1 AS c1
  
  FROM Reformat_2_1 AS in0
  INNER JOIN SQLStatement_1_2 AS in1
     ON in0.customer_id != in1.c1

),

env_uitesting_shared_mid_model_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_shared_mid_model_1')}}

),

Join_1 AS (

  SELECT 
    in0.c_tinyint AS c_tinyint,
    in0.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in0.c_bigint AS c_bigint,
    in0.c_float AS c_float,
    in0.c_double AS c_double,
    in0.c_string AS c_string,
    in0.c_boolean AS c_boolean,
    in0.c_array AS c_array,
    in0.c_struct AS c_struct
  
  FROM Limit_6 AS in0
  INNER JOIN env_uitesting_shared_mid_model_1 AS in1
     ON in0.c_smallint != in1.c_int
  INNER JOIN Join_1_2 AS in2
     ON in1.c_string != in2.customer_id

),

env_uitesting_main_model_databricks_1 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_main_model_databricks_1')}}

),

Limit_7 AS (

  SELECT * 
  
  FROM env_uitesting_main_model_databricks_1 AS in0
  
  LIMIT 10

),

all_type_partitioned AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'all_type_partitioned') }}

),

SQLStatement_1 AS (

  SELECT *
  
  FROM all_type_partitioned
  
  WHERE c_int != (
          SELECT count(*)
          
          FROM hive_metastore.qa_database.tpcds_uitesting_shared_1
         )
  
  LIMIT 100

),

OrderBy_2 AS (

  SELECT * 
  
  FROM SQLStatement_1 AS in0
  
  ORDER BY concat(c_string, c_int) ASC, c_tinyint DESC NULLS FIRST

),

payments AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'payments') }}

),

Reformat_2 AS (

  SELECT 
    ID AS ID,
    ORDER_ID AS ORDER_ID,
    PAYMENT_METHOD AS `PAYMENT_METHOD`,
    AMOUNT AS AMOUNT
  
  FROM payments AS in0

),

orders AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'orders') }}

),

Reformat_3 AS (

  SELECT 
    ID AS ID,
    USER_ID AS USER_ID,
    ORDER_DATE AS ORDER_DATE,
    STATUS AS STATUS
  
  FROM orders AS in0

),

time_dim AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'time_dim') }}

),

Reformat_4 AS (

  SELECT 
    T_TIME_SK AS T_TIME_SK,
    T_TIME_ID AS T_TIME_ID,
    T_TIME AS T_TIME,
    T_HOUR AS T_HOUR,
    T_MINUTE AS T_MINUTE,
    T_SECOND AS T_SECOND,
    T_AM_PM AS T_AM_PM,
    T_SHIFT AS T_SHIFT,
    T_SUB_SHIFT AS T_SUB_SHIFT,
    T_MEAL_TIME AS T_MEAL_TIME
  
  FROM time_dim AS in0

),

income_band AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'income_band') }}

),

SQLStatement_4 AS (

  SELECT 
    DISTINCT IB_INCOME_BAND_SK,
    IB_Lower_boUND,
    IB_UPPER_BOUND
  
  FROM Income_BAND

),

Reformat_5 AS (

  SELECT 
    IB_INCOME_BAND_SK AS IB_INCOME_BAND_SK,
    IB_LOWER_BOUND AS IB_LOWER_BOUND,
    IB_UPPER_BOUND AS IB_UPPER_BOUND
  
  FROM SQLStatement_4 AS in0

),

store_returns AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_suggestion_database', 'store_returns') }}

),

SQLStatement_4_1 AS (

  SELECT 
    DISTINCT SR_RETURNED_Date_SK,
    SR_RETURN_TIME_SK,
    SR_RETURN_amt_INC_TAX
  
  FROM Store_RETURNS

),

Reformat_6 AS (

  SELECT 
    SR_RETURNED_DATE_SK AS SR_RETURNED_DATE_SK,
    SR_RETURN_TIME_SK AS SR_RETURN_TIME_SK
  
  FROM SQLStatement_4_1 AS in0

),

combine_multiple_tables_2 AS (

  {{ SQL_DatabricksSharedBasic.combine_multiple_tables(table_1 = 'Reformat_5', table_2 = 'Reformat_6', table_3 = 'Reformat_4', table_4 = 'Reformat_3', table_5 = 'Reformat_2', col_table_1 = 'IB_LOWER_BOUND') }}

),

model_with_only_seed_base AS (

  SELECT * 
  
  FROM {{ ref('model_with_only_seed_base')}}

),

Join_3 AS (

  SELECT 
    in1.p_int AS p_int,
    in1.p_string AS p_string,
    in1.c_string AS c_string,
    in1.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in1.c_smallint AS c_smallint,
    in1.c_tinyint AS c_tinyint,
    in1.c_float AS c_float,
    in1.c_boolean AS c_boolean,
    in1.c_array AS c_array,
    in1.c_double AS c_double,
    in1.c_struct AS c_struct,
    in1.c_struct.city AS c_struct_city,
    in1.c_struct.state AS c_struct_state,
    in1.c_struct.pin AS c_struct_pin
  
  FROM model_with_only_seed_base AS in0
  INNER JOIN Limit_7 AS in1
     ON in0.country_code != in1.p_string
  LEFT JOIN Join_1_1 AS in2
     ON in1.p_string != in2.sm_type
  RIGHT JOIN OrderBy_2 AS in3
     ON in2.sm_type != in3.p_string
  SEMI JOIN combine_multiple_tables_2 AS in4
     ON in3.p_string != CAST(in4.IB_INCOME_BAND_SK AS string)

),

Limit_1 AS (

  SELECT * 
  
  FROM Filter_1 AS in0
  
  LIMIT 100

),

Limit_2 AS (

  SELECT * 
  
  FROM Join_1 AS in0
  
  LIMIT 25

),

Limit_3 AS (

  SELECT * 
  
  FROM Join_3 AS in0
  
  LIMIT 10

),

OrderBy_1 AS (

  SELECT * 
  
  FROM Limit_1 AS in0
  
  ORDER BY first_name ASC NULLS FIRST

),

Limit_4 AS (

  SELECT * 
  
  FROM OrderBy_1 AS in0
  
  LIMIT 15

),

Limit_4_1 AS (

  SELECT * 
  
  FROM SQLStatement_1 AS in0
  
  LIMIT 5

),

Limit_5 AS (

  SELECT * 
  
  FROM Aggregate_1 AS in0
  
  LIMIT 10

),

SetOperation_1 AS (

  SELECT * 
  
  FROM OrderBy_2 AS in0
  
  INTERSECT
  
  SELECT * 
  
  FROM Limit_4_1 AS in1

),

SetOperation_2 AS (

  SELECT * 
  
  FROM OrderBy_2 AS in0
  
  UNION
  
  SELECT * 
  
  FROM SetOperation_1 AS in1

),

SetOperation_3 AS (

  SELECT * 
  
  FROM OrderBy_2 AS in0
  
  EXCEPT
  
  SELECT * 
  
  FROM SetOperation_2 AS in1

),

combine_multiple_tables_1 AS (

  {{ SQL_DatabricksSharedBasic.combine_multiple_tables(table_1 = 'Limit_2', table_2 = 'Limit_5', table_3 = 'Limit_4', table_4 = 'Limit_3', table_5 = 'SetOperation_3', col_table_1 = 'c_int') }}

),

tpcds_uitesting_shared_1 AS (

  SELECT * 
  
  FROM {{ ref('tpcds_uitesting_shared_1')}}

)

SELECT *

FROM combine_multiple_tables_1

{% if is_incremental() %}
  WHERE 
    c_bigint > 10
{% endif %}