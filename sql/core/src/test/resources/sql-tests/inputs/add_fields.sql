CREATE TEMPORARY VIEW struct_level_1 AS VALUES
  (1, CAST(STRUCT(1, NULL, 3) AS struct<a:int,b:int,c:int>))
  AS T(id, a);

CREATE TEMPORARY VIEW null_struct_level_1 AS VALUES
  (CAST(NULL AS struct<a:int,b:int,c:int>))
  AS T(a);

CREATE TEMPORARY VIEW struct_level_2 AS VALUES
  (CAST(STRUCT(STRUCT(1, NULL, 3)) AS struct<a:struct<a:int,b:int,c:int>>))
  AS T(a);

-- Should fail if first argument is not a struct
SELECT ADD_FIELDS(id, 'd', 4) AS a FROM struct_level_1;

-- Should fail if fieldName is not a string
SELECT ADD_FIELDS(a, 1, 2) AS a FROM struct_level_1;

-- Should fail if fieldName is null
SELECT ADD_FIELDS(a, NULL, 2) AS a FROM struct_level_1;

-- Should fail if name-value pairs aren't given
SELECT ADD_FIELDS(a, 'd') AS a FROM struct_level_1;

-- Should return original struct if given no fields to add/replace
SELECT ADD_FIELDS(a) AS a FROM struct_level_1;

-- Should add field to struct
SELECT ADD_FIELDS(a, 'd', 4) AS a FROM struct_level_1;

-- Should add field to null struct
SELECT ADD_FIELDS(a, 'd', 4) a FROM null_struct_level_1;

-- Should add field with null value to struct
SELECT ADD_FIELDS(a, 'd', NULL) AS a FROM struct_level_1;

-- Should add multiple fields to struct
SELECT ADD_FIELDS(a, 'd', 4, 'e', 5) AS a FROM struct_level_1;

-- Should add field to nested struct
SELECT ADD_FIELDS(a, 'a', ADD_FIELDS(a.a, 'd', 4)) AS a FROM struct_level_2;

-- Should replace field in struct
SELECT ADD_FIELDS(a, 'b', 2) AS a FROM struct_level_1;

-- Should replace field in null struct
SELECT ADD_FIELDS(a, 'b', 2) AS a FROM null_struct_level_1;

-- Should replace field with null value in struct
SELECT ADD_FIELDS(a, 'c', NULL) AS a FROM struct_level_1;

-- Should replace multiple fields in struct
SELECT ADD_FIELDS(a, 'a', 10, 'b', 20) AS a FROM struct_level_1;

-- Should replace field in nested struct
SELECT ADD_FIELDS(a, 'a', ADD_FIELDS(a.a, 'b', 2)) AS a FROM struct_level_2;

-- Should replace all fields with given name in struct
SELECT ADD_FIELDS(a, 'b', 100) AS a FROM (VALUES (NAMED_STRUCT('a', 1, 'b', 2, 'b', 3)) AS nested_struct(a));

-- Should replace field in struct in given order
SELECT ADD_FIELDS(a, 'b', 2, 'b', 20) AS a FROM struct_level_1;

-- Should add and replace fields in struct
SELECT ADD_FIELDS(a, 'b', 2, 'd', 4) AS a FROM struct_level_1;