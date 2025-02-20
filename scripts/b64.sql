SELECT * FROM episodes WHERE URL LIKE '%.mp4';
SELECT COUNT(*) FROM episodes WHERE URL LIKE '%.mp4';

ALTER TABLE metadata DROP COLUMN record_type;
ALTER TABLE episodes DROP COLUMN record_type;

SELECT COUNT(*) FROM streams;
SELECT COUNT(*) FROM episodes;
SELECT COUNT(*) FROM metadata;
SELECT COUNT(*) FROM sites;

SELECT * FROM  sqlite_master;

SELECT URL FROM metadata WHERE deprecated = 0 GROUP BY URL HAVING COUNT(*) > 2;

VACUUM;

SELECT * FROM streams;

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

SELECT * FROM episodes GROUP BY DATE(found);

SELECT * FROM sqlite_master;

SELECT found FROM metadata ORDER BY found DESC LIMIT 5;

SELECT * FROM metadata WHERE URL = 'https://www.video.ethz.ch/speakers/introductory_lectures.series-metadata.json';

SELECT URL, parent FROM metadata WHERE deprecated = 0 AND key NOT IN (SELECT key FROM temp);

SELECT * FROM metadata WHERE URL = 'https://www.video.ethz.ch/lectures/d-baug/2021/spring/101-0588-01L.series-metadata.json' AND parent = 3263;

SELECT * FROM metadata WHERE json LIKE 'ey%';

-- Create a table of all the diff records that are correctly set to not deprecated
CREATE TABLE temp AS WITH LatestRecord AS (
    SELECT URL, parent, MAX(DATETIME(found)) AS latest_found
    FROM metadata
    WHERE record_type = 1
    GROUP BY URL, parent
)
SELECT t.key
FROM metadata t
JOIN LatestRecord lr ON t.URL = lr.URL AND t.found = lr.latest_found AND t.parent = lr.parent
WHERE t.record_type = 1 AND deprecated = 0; -- 2287


DROP TABLE IF EXISTS temp;

SELECT COUNT(*) FROM temp;

-- Get the records which are initial only in the db and are also not deprecated
INSERT INTO temp SELECT key FROM metadata WHERE deprecated = 0 AND record_type = 0 AND key IN (SELECT key FROM metadata GROUP BY parent, URL HAVING COUNT(*) = 1); -- 2207

-- Get the records which are final in the db, are in a group of at least three and are not deprecated
INSERT INTO temp SELECT key FROM metadata WHERE deprecated = 0 AND record_type = 2 AND URL IN (SELECT URL FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2) AND parent IN (SELECT parent FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2);
SELECT key FROM metadata WHERE deprecated = 0 AND record_type = 2 AND URL IN (SELECT URL FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2) AND parent IN (SELECT parent FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2);

SELECT COUNT(*) FROM (SELECT COUNT(*) FROM metadata GROUP BY URL, parent HAVING COUNT(*) = 1);
SELECT COUNT(*) FROM (SELECT COUNT(*) FROM metadata GROUP BY URL, parent HAVING COUNT(*) > 1);

SELECT URL, parent FROM (SELECT COUNT(*), * FROM metadata GROUP BY URL, parent HAVING COUNT(*) = 1);
SELECT URL, parent FROM (SELECT COUNT(*), * FROM metadata GROUP BY URL, parent HAVING COUNT(*) > 1);

SELECT * FROM metadata WHERE DATETIME(found) > DATETIME('2024-09-24 00:00:00') AND record_type = 1;
SELECT * FROM sites WHERE URL LIKE '%lectures/d-infk/2024%';
SELECT * FROM main.metadata WHERE URL LIKE 'https://video.ethz.ch/lectures/d-infk/2024/autumn/252-0206-00L%';

-- SELECT * FROM metadata WHERE parent = 6582;
SELECT * FROM metadata WHERE parent = 6527;

UPDATE metadata SET deprecated = 0;
UPDATE metadata SET deprecated = 1 WHERE datetime(last_seen) < datetime('2024-12-14 00:00:00');

SELECT * FROM metadata_episode_assoz AS m JOIN episodes AS e ON m.episode_key = e.key WHERE m.metadata_key IN (10805, 11029, 11512, 11241);
SELECT * FROM metadata_episode_assoz AS m JOIN episodes AS e ON m.episode_key = e.key WHERE m.metadata_key IN
                                                                                        (13125, 13336, 11512, 13832);

SELECT * FROM episodes WHERE datetime(found) > datetime('2024-09-24 00:00:00');

SELECT * FROM episodes WHERE record_type = 1 AND datetime(found) > datetime('2024-12-14 00:00:00');

SELECT key, json, URL, record_type, last_seen, parent FROM metadata WHERE deprecated = 0 AND record_type IN (0, 2) AND key in (SELECT key FROM metadata WHERE parent = 6527);


SELECT * FROM sites WHERE key = 96;
SELECT * FROM sites WHERE parent = 96;
SELECT * FROM sites WHERE parent = 6297;
SELECT * FROM sites WHERE URL = 'https://www.video.ethz.ch/speakers/qsit.html';

UPDATE sites SET parent = 6297 WHERE parent = 96;
-- new qsit parent 6297

SELECT * FROM sites ORDER BY found DESC;