-- Total users loaded
SELECT COUNT(*) FROM raw_users;

-- Total songs loaded
SELECT COUNT(*) FROM raw_songs;

-- Total stream records
SELECT COUNT(*) FROM transformed_streams;

-- Duplicate users
SELECT user_id, COUNT(*) 
FROM raw_users 
GROUP BY user_id 
HAVING COUNT(*) > 1;

-- Duplicate songs
SELECT track_id, COUNT(*) 
FROM raw_songs 
GROUP BY track_id 
HAVING COUNT(*) > 1;

-- Streams with missing users or songs
SELECT *
FROM transformed_streams t
LEFT JOIN raw_users u ON t.user_id = u.user_id
LEFT JOIN raw_songs s ON t.track_id = s.track_id
WHERE u.user_id IS NULL OR s.track_id IS NULL;


-- Top artist per hour KPI

SELECT DISTINCT
    t.hour,
    s.artists,
    COUNT(*) OVER (PARTITION BY t.hour, s.artists) AS play_count
FROM
    transformed_streams t
JOIN
    processed_songs s ON t.track_id = s.track_id
ORDER BY
    t.hour, play_count DESC;