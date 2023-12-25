WITH cte_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM
        dep_table_1
)

SELECT
    cte_count.cnt = 2
  , cte_count.cnt
FROM
    cte_count
;
