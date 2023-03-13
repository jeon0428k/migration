SELECT YEARWEEK(NOW(), 1) AS current_week, DATE_FORMAT(NOW(), '%Y-%m-%d') AS date_column
;

SELECT week
     , start_date
     , start_day_name
     , end_date
     , end_day_name
     , current_date          AS curr_date
     , DAYNAME(current_date) AS curr_day_name
FROM (SELECT YEARWEEK(date_column, 1)  AS week
           , MIN(date_column)          AS start_date
           , DAYNAME(MIN(date_column)) AS start_day_name
           , MAX(date_column)          AS end_date
           , DAYNAME(MAX(date_column)) AS end_day_name
      FROM (SELECT date_column
                 , DAYNAME(date_column)   AS day_name
                 , DAYOFWEEK(date_column) AS day_of_week
                 , WEEK(date_column, 1)   AS week
                 , DAY(date_column)       AS day
                 , MONTH(date_column)     AS month
                 , YEAR(date_column)      AS year
            FROM (SELECT DATE('2023-01-01') + INTERVAL t4 + t16 + t64 + t256 + t1024 + t4096 DAY AS date_column
                  FROM (SELECT 0 t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) t4,
                       (SELECT 0 t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12) t16,
                       (SELECT 0 t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48) t64,
                       (SELECT 0 t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192) t256,
                       (SELECT 0 t1024 UNION SELECT 256 UNION SELECT 512 UNION SELECT 768) t1024,
                       (SELECT 0 t4096 UNION SELECT 1024 UNION SELECT 2048 UNION SELECT 3072) t4096) dates
            WHERE YEAR(date_column) = 2023
            ORDER BY date_column) dates
      WHERE YEAR(date_column) = 2023
      GROUP BY YEAR(date_column), WEEK(date_column, 1)
      ORDER BY week) week_dates
WHERE 1 = 1
  AND current_date BETWEEN start_date AND end_date
;