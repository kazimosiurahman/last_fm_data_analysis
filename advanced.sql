-- provided SQL query

SELECT
    historical.app_id,
    historical.retention_date,
    COUNT(historical.user_id) AS users_start,
    COUNT(current.user_id) AS users_retained,
    CAST(COUNT(current.user_id) AS DOUBLE) /
    COUNT(historical.user_id) AS percent_retained
FROM   (
            SELECT DISTINCT app_id, user_id,
            event_date + INTERVAL '7' DAY AS retention_date
            FROM app_user_events
            WHERE event = 'app_open'
        ) historical
        LEFT JOIN 
        (
            SELECT DISTINCT app_id, user_id, event_date
            FROM app_user_events
            WHERE event = 'app_open'
        ) current
ON 
    historical.app_id = current.app_id
    AND historical.user_id = current.user_id
    AND historical.retention_date = current.event_date
    GROUP BY historical.app_id, historical.retention_date

-- the subqueries reduce the performance 
-- the subqueries are generated to join on N days delayed date
-- the N days delayed joining can be set on the ON clause to reduce redundancy
-- post left join , the historical alias will have dates post the Current date for which current alias have null values 
-- the historical dates post the current date can be filtered to optimise the performance further 

SELECT
    historical.app_id,
    historical.retention_date,
    COUNT(distinct historical.user_id) AS users_start,
    COUNT(distinct current.user_id) AS users_retained,
    CAST(COUNT(distinct current.user_id) AS DOUBLE) /
    COUNT(distinct historical.user_id) AS percent_retained
FROM 
    event historical
    left join
    event current
ON  
    historical.app_id = current.app_id
    AND historical.user_id = current.user_id
    AND historical.event_date + INTERVAL '7' = current.event_date
    AND historical.event_date + INTERVAL '7' <= CURDATE()
    AND historical.event = 'app_open'
    AND current.event = 'app_open'
    GROUP BY historical.app_id, historical.retention_date
