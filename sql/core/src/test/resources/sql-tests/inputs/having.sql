create temporary view hav as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", 5)
  as hav(k, v);

-- having clause
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2;

-- having condition contains grouping column
SELECT count(k) FROM hav GROUP BY v + 1 HAVING v + 1 = 2;

-- SPARK-11032: resolve having correctly
SELECT MIN(t.v) FROM (SELECT * FROM hav WHERE v > 0) t HAVING(COUNT(1) > 0);

-- SPARK-20329: make sure we handle timezones correctly
SELECT a + b FROM VALUES (1L, 2), (3L, 4) AS T(a, b) GROUP BY a + b HAVING a + b > 1;

-- SPARK-33131: Grouping sets with having clause can not resolve qualified col name.
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY GROUPING SETS(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY CUBE(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY ROLLUP(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY t.c1 HAVING t.c1 = 1;