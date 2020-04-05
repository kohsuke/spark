CREATE TEMPORARY VIEW struct_level_1 AS VALUES
  (1, NAMED_STRUCT('A', 1, 'B', 1, 'C', 1))
  AS T(ID, A);

CREATE TEMPORARY VIEW null_struct_level_1 AS VALUES
  (CAST(NULL AS struct<A:int,B:int,C:int>))
  AS T(A);

CREATE TEMPORARY VIEW struct_level_2 AS VALUES
  (NAMED_STRUCT('A', NAMED_STRUCT('A', 1, 'B', 1, 'C', 1)))
  AS T(A);

-- Should fail if first argument is not a struct
SELECT ADD_FIELDS(ID, 'D', 2) AS A FROM struct_level_1;

-- Should fail if fieldName is not a string
SELECT ADD_FIELDS(A, 1, 2) AS A FROM struct_level_1;

-- Should fail if given a null string for fieldName
SELECT ADD_FIELDS(A, NULL, 2) AS A FROM struct_level_1;

-- Should fail if name-value pairs aren't given
SELECT ADD_FIELDS(A, 'D') AS A FROM struct_level_1;

-- Should return original struct if given no fields to add/replace
SELECT ADD_FIELDS(A) AS A FROM struct_level_1;

-- Should return null if struct is null
SELECT ADD_FIELDS(A, 'D', 2) AS A FROM null_struct_level_1;

-- Should add new field to struct
SELECT ADD_FIELDS(A, 'D', 2) AS A FROM struct_level_1;

-- Should add new field with null value to struct
SELECT ADD_FIELDS(A, 'D', NULL) AS A FROM struct_level_1;

-- Should add multiple new fields to struct
SELECT ADD_FIELDS(A, 'D', 2, 'E', 3) AS A FROM struct_level_1;

-- Should replace field in struct
SELECT ADD_FIELDS(A, 'B', 2) AS A FROM struct_level_1;

-- Should replace field with null value in struct
SELECT ADD_FIELDS(A, 'B', NULL) AS A FROM struct_level_1;

-- Should replace multiple fields in struct
SELECT ADD_FIELDS(A, 'A', 2, 'B', 2) AS A FROM struct_level_1;

-- Should add and replace fields in struct
SELECT ADD_FIELDS(A, 'B', 2, 'D', 2) AS A FROM struct_level_1;

-- Should add new field to nested struct
SELECT ADD_FIELDS(A, 'A', ADD_FIELDS(A.A, 'D', 2)) AS A FROM struct_level_2;

-- Should replace field in nested struct
SELECT ADD_FIELDS(A, 'A', ADD_FIELDS(A.A, 'B', 2)) AS A FROM struct_level_2;