-- This test case just used to test imported bracketed comments.

-- the first case of bracketed comment
--QUERY-DELIMITER-START
/* This is the first example of bracketed comment.
SELECT 'ommented out content' AS first;
*/
SELECT 'selected content' AS first;
--QUERY-DELIMITER-END
