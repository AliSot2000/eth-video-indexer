# eth-video-indexer
Create an index of the entire video site of eth. Given username and password for ldap / for a specific series or lecture, indexer can also fetch the url of all video files available on the eth video servers.

Commands:
```bash
rsync -avu --exclude sample\ text/ --exclude scratch/ --exclude __pycache__/ --exclude db_backup/ --exclude logs/ --exclude lin_venv/ /home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/ piora:/home/asotoude/projects/eth-video-indexer/
```

## Table Specs:
- `sites` : Contains the list of all the sites that have been indexed.
- `metadata` : Contains the series_metadata.json of the html file.
- `episodes`: Contains the series_metadata of every episode-id
- `metadata_episode_assoz`: Association between episode-key and metadata-key
- `streams`: Contains all video files that could be indexed by the crawler.
- `episode_stream_assoz`: Association between episode-key and stream-key

### Table 1: `sites`
```sqlite
CREATE TABLE sites 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 parent INTEGER, 
 URL TEXT, 
 IS_VIDEO INTEGER CHECK (IS_VIDEO IN (0, 1)),
 found TEXT,
 last_seen TEXT, UNIQUE (URL, IS_VIDEO))
```
*Indexes on:*
- `URL`
- `key`
- `parent`

### Table 2: `metadata`
```sqlite
CREATE TABLE metadata 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 parent INTEGER, -- Reference to the entry in the the sites table
 URL TEXT , 
 json TEXT,
 deprecated INTEGER DEFAULT 0 CHECK (metadata.deprecated IN (0, 1)),
 found TEXT,
 last_seen TEXT)
```
*Indexes on:*
- `key`
- `(URL, parent)`

### Table 3: `episodes`
```sqlite
CREATE TABLE episodes 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 URL TEXT , 
 json TEXT,
 deprecated INTEGER DEFAULT 0 CHECK (episodes.deprecated >= 0 AND episodes.deprecated <= 1),
 found TEXT,
 last_seen TEXT)
```

*Indexes on:*
- `key`
- `URL`

### Table 4: `metadata_episode_assoz`
```sqlite
CREATE TABLE metadata_episode_assoz 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 metadata_key INTEGER REFERENCES metadata(key), 
 episode_key INTEGER REFERENCES episodes(key), UNIQUE (metadata_key, episode_key))
```

*Indexes on:*
- `(metadata_key, episode_key)`

### Table 5: `streams`
```sqlite
CREATE TABLE streams 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 URL TEXT , 
 resolution TEXT,
 deprecated INTEGER DEFAULT 0 CHECK (deprecated >= 0 AND deprecated <= 1),
 found TEXT, 
 last_seen TEXT)
```

*Indexes on:*
- `key`
- `(resolution, URL)`

### Table 6: `episode_stream_assoz`
```sqlite
CREATE TABLE episode_stream_assoz 
(key INTEGER PRIMARY KEY AUTOINCREMENT, 
 episode_key INTEGER REFERENCES episodes(key), 
 stream_key INTEGER REFERENCES streams(key), UNIQUE (episode_key, stream_key))
```

*Indexes on:*
- `(episode_key, stream_key)`