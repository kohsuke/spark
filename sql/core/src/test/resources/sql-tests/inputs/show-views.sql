-- Test data.
CREATE DATABASE showdb;
USE showdb;
CREATE TABLE tbl(a String, b Int, c String, d String) USING parquet;
CREATE VIEW view_1 AS SELECT * FROM tbl;
CREATE VIEW view_2 AS SELECT * FROM tbl WHERE c='a';
CREATE TEMPORARY VIEW view_3(e int) USING parquet;
CREATE GLOBAL TEMP VIEW view_4 AS SELECT 1 as col1;

-- SHOW VIEWS
SHOW VIEWS;
SHOW VIEWS FROM showdb;
SHOW VIEWS IN showdb;
SHOW VIEWS IN global_temp;

-- SHOW VIEWS WITH wildcard match
SHOW VIEWS 'view_*';
SHOW VIEWS LIKE 'view_1*|view_2*';
SHOW VIEWS IN showdb 'view_*';
SHOW VIEWS IN showdb LIKE 'view_*';
-- Error when database not exists
SHOW VIEWS IN wrongdb LIKE 'view_*';

-- Clean Up
DROP VIEW view_1;
DROP VIEW view_2;
DROP VIEW view_3;
DROP VIEW global_temp.view_4;
DROP TABLE tbl;
USE default;
DROP DATABASE showdb;
