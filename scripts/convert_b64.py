from eth_loader.metadata_loader import EpisodeLoader
from eth_loader.stream_loader import BetterStreamLoader
import base64 as b64
import json as js

def to_b64(s: str):
    """
    Convert string to base64
    :param s: string to convert
    :return:
    """
    return b64.b64encode(s.encode("utf-8")).decode("utf-8")


def from_b64(s: str):
    """
    Convert base64 to string
    :param s: base64 to convert
    :return:
    """
    return b64.b64decode(s.encode("utf-8")).decode("utf-8")


def convert_metadata(p: str):
    obj = EpisodeLoader(p)
    obj.sq_cur.execute("SELECT key FROM metadata")
    only_keys = [k[0] for k in obj.sq_cur.fetchall()]

    for k in only_keys:
        obj.sq_cur.execute(f"SELECT json FROM metadata WHERE key = {k}")
        json = obj.sq_cur.fetchone()[0]
        json = json.replace("''", "'")

        b64_json = to_b64(json)
        obj.sq_cur.execute(f"UPDATE metadata SET json = '{b64_json}' WHERE key = {k}")

    obj.sq_con.commit()


def convert_episodes(p: str):
    obj = BetterStreamLoader(p)
    obj.sq_cur.execute("SELECT key FROM episodes")
    ok = [k[0] for k in obj.sq_cur.fetchall()]

    for k in ok:
        if k % 1000 == 0:
            print(k)
            obj.sq_con.commit()
        obj.sq_cur.execute(f"SELECT json from episodes WHERE key = {k}")
        json = obj.sq_cur.fetchone()[0]
        json = json.replace("''", "'")

        b64_json = to_b64(json)
        obj.sq_cur.execute(f"UPDATE episodes SET json = '{b64_json}' WHERE key = {k}")

    obj.sq_con.commit()
def convert_streams(p: str):
    obj = BetterStreamLoader(p, verify_tbl=False)
    obj.debug_execute("DROP TABLE IF EXISTS episode_stream_assoz")
    obj.debug_execute("SELECT * FROM sqlite_master WHERE type='table' AND name='temp'")
    if obj.sq_cur.fetchone() is None:
        obj.debug_execute("ALTER TABLE episodes RENAME TO temp")
    obj.debug_execute("CREATE TABLE IF NOT EXISTS episodes AS SELECT key, parent, URL, json, deprecated, found, last_seen FROM temp")
    obj.check_results_table()
    obj.sq_cur.execute("SELECT key FROM episodes")
    ok = [k[0] for k in obj.sq_cur.fetchall()]

    for k in ok:
        if k % 100 == 0:
            print(k)
            obj.sq_con.commit()
        obj.sq_cur.execute(f"SELECT streams from temp WHERE key = {k}")
        streams = obj.sq_cur.fetchone()[0]

        s = js.loads(streams)
        for stream in s:
            if type(stream) is not list:
                assert type(stream) is int, "type of element not int"
                obj.debug_execute(f"INSERT OR IGNORE INTO episode_stream_assoz (episode_key, stream_key) VALUES ({k}, {stream})")
            else:
                obj.debug_execute(f"INSERT OR IGNORE INTO episode_stream_assoz (episode_key, stream_key) VALUES ({k}, {stream[0]})")

    obj.debug_execute("DROP TABLE temp")
    obj.sq_con.commit()

if __name__ == "__main__":
    print("PASS")
    # UPDATE TO B64 for one
    path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites.db"
    # convert_metadata(path)
    # convert_episodes(path)
    convert_streams(path)
    print("Done")
