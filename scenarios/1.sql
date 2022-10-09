-- TASK 1
CREATE TABLE `table_a` AS 
SELECT *
FROM `file_one_content`

-- TASK 2
CREATE TABLE `table_b` as 
SELECT *
FROM `file_two_content`

-- TASK 3
CREATE TABLE `table_c` AS 
SELECT b.*
FROM table_b AS b
INNER JOIN table_a AS a
ON a.id = b.id

-- TASK 4
SELECT c.* 
FROM table_c AS c
LEFT JOIN table_b AS b
ON b.id = c.id
WHERE b.id IS NULL