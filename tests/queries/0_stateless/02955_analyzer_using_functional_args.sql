CREATE TABLE t1 (x Int16, y Int64 ALIAS x + x * 2, z ALIAS x + 1, s String) ENGINE=MergeTree() ORDER BY x;
CREATE TABLE t2 (y Int128, z Int16) ENGINE=MergeTree() ORDER BY y;

CREATE TABLE t3 (x Int16, y Int64 ALIAS x + x * 2, z ALIAS x + 1) ENGINE=MergeTree() ORDER BY x;

INSERT INTO t1 VALUES (1231, 'a'), (123, 'b');

INSERT INTO t2 VALUES (6666, 48);
INSERT INTO t2 VALUES (369, 124);

INSERT INTO t3 VALUES (1231), (42);

SELECT count() FROM t1 INNER JOIN t2 USING (y);
SELECT count() FROM t2 INNER JOIN t1 USING (y);

-- `SELECT *` works differently for ALIAS columns with analyzer
SELECT * FROM t1 INNER JOIN t2 USING (y, z) SETTINGS allow_experimental_analyzer = 1;
SELECT * FROM t2 INNER JOIN t1 USING (y, z) SETTINGS allow_experimental_analyzer = 1;
SELECT t2.z FROM t1 INNER JOIN t2 USING (y);

SELECT * FROM t1 INNER JOIN t3 USING (y) SETTINGS allow_experimental_analyzer = 1;
SELECT * FROM t3 INNER JOIN t1 USING (y, z) SETTINGS allow_experimental_analyzer = 1;
SELECT s FROM t1 INNER JOIN t3 USING (y);

-- {echoOn }
SELECT y * 2, s || 'a' FROM t1 FULL JOIN t2 USING (y) ORDER BY ALL SETTINGS allow_experimental_analyzer = 1;
SELECT y * 2, s || 'a' FROM (SELECT s, y FROM t1) t1 FULL JOIN (SELECT y FROM t2) t2 USING (y) ORDER BY ALL;

SELECT y FROM t1 FULL JOIN t2 USING (y) ORDER BY ALL SETTINGS allow_experimental_analyzer = 1;
SELECT y FROM (SELECT s, y FROM t1) t1 FULL JOIN (SELECT y FROM t2) t2 USING (y) ORDER BY ALL;

SELECT s FROM t1 FULL JOIN t2 USING (y) ORDER BY ALL;
SELECT s FROM (SELECT s, y FROM t1) t1 FULL JOIN (SELECT y FROM t2) t2 USING (y) ORDER BY ALL;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
