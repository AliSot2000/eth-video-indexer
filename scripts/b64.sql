SELECT * FROM episodes WHERE URL LIKE '%.mp4';

ALTER TABLE metadata DROP COLUMN record_type;
ALTER TABLE episodes DROP COLUMN record_type;

SELECT COUNT(*) FROM episodes WHERE URL LIKE '%.mp4' -- 41643

SELECT COUNT(*) FROM streams;
SELECT COUNT(*) FROM episodes;
SELECT COUNT(*) FROM metadata;
SELECT COUNT(*) FROM sites;

SELECT * FROM  sqlite_master;


UPDATE metadata SET record_type = 0 WHERE URL IN (SELECT URL FROM metadata GROUP BY URL HAVING COUNT(URL) = 1);
UPDATE episodes SET record_type = 0 WHERE URL IN (SELECT URL FROM episodes GROUP BY URL HAVING COUNT(URL) = 1);

SELECT URL FROM metadata WHERE deprecated = 0 GROUP BY URL HAVING COUNT(*) > 2;

VACUUM;

SELECT COUNT(*) FROM episodes WHERE key > 190000;

SELECT MAX(key) FROM episodes;

SELECT COUNT(*) AS CNT, * FROM episodes GROUP BY URL HAVING COUNT(*) > 1 ORDER BY CNT DESC;
SELECT COUNT(*) AS CNT, * FROM metadata GROUP BY URL HAVING COUNT(*) > 1 ORDER BY CNT DESC;

SELECT * FROM episodes WHERE URL = 'https://www.video.ethz.ch/speakers/lecture/00031a77-072e-4b96-8175-ba3fff5bbe37.series-metadata.json';
SELECT * FROM metadata WHERE URL = 'https://www.video.ethz.ch/speakers/lecture.series-metadata.json';


CREATE TABLE temp AS SELECT episodes.key AS key
                     FROM metadata JOIN metadata_episode_assoz ON metadata.key = metadata_episode_assoz.metadata_key
                         JOIN episodes ON metadata_episode_assoz.episode_key = episodes.key
                     WHERE metadata.deprecated = 0
                       AND datetime(metadata.last_seen) >= datetime('2024-09-10 00:00:00')
                       AND datetime(episodes.last_seen) >= datetime('2024-09-10 00:00:00');

INSERT INTO temp SELECT episodes.key AS key FROM episodes
                     WHERE episodes.record_type = 2
                       AND datetime(episodes.last_seen) >= datetime('2024-09-10 00:00:00');

SELECT COUNT(episodes.key) FROM episodes
                     WHERE episodes.record_type = 2
                       AND datetime(episodes.last_seen) >= datetime('2024-09-10 00:00:00');

UPDATE episodes SET deprecated = 0 WHERE DATETIME(last_seen) >= datetime('2024-09-10 00:00:00') AND record_type = 2;;


SELECT COUNT(metadata.key) FROM metadata
                     WHERE metadata.record_type = 2
                       AND datetime(metadata.last_seen) >= datetime('2024-09-10 00:00:00');

UPDATE metadata SET deprecated = 0 WHERE DATETIME(last_seen) >= datetime('2024-09-10 00:00:00') AND record_type = 2;;

SELECT * FROM episodes GROUP BY DATE(found);

SELECT * FROM sqlite_master;