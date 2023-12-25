WITH cte_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM
        dep_table_0
)

SELECT
    cte_count.cnt = 1
  , cte_count.cnt
FROM
    cte_count
;

