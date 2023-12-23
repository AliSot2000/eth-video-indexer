import sys

from eth_loader.base_sql import BaseSQliteDB


class MiddlePruner(BaseSQliteDB):
    def __init__(self, db: str):
        super().__init__(db)

    def create_hash_table(self):
        self.debug_execute("DROP TABLE IF EXISTS hash_table")
        self.debug_execute("CREATE TABLE hash_table (key INTEGER PRIMARY KEY, hash TEXT)")
        self.debug_execute("SELECT key, json FROM episodes ORDER BY key ASC")
        row_0 = self.sq_cur.fetchone()

        while row_0 is not None:
            key = row_0[0]
            json = row_0[1]
            json = json.replace("'", "''")
            self.debug_execute(f"INSERT INTO hash_table VALUES ({key}, '{hash(json)}')")
            print(f"Processed {key}", flush=True, end="\r")
            self.debug_execute(f"SELECT key, json FROM episodes WHERE key > {key} ORDER BY key ASC")
            row_0 = self.sq_cur.fetchone()


    def perform_cleaning_episodes(self):
        self.debug_execute("SELECT COUNT(episodes.key), parent, hash_table.hash FROM episodes JOIN hash_table ON episodes.key = hash_table.key GROUP BY hash_table.hash HAVING COUNT(episodes.key) > 1")
        raw = self.sq_cur.fetchall()
        results = [{"count": row[0], "parent": row[1], "hash": row[2]} for row in raw]

        equal_streams = 0
        unequal_streams = 0

        unequal_json = 0
        nor = len(results)

        for r in results:
            fhash = r["hash"]
            self.debug_execute(f"SELECT episodes.key, json FROM episodes JOIN hash_table ON episodes.key = hash_table.key WHERE hash_table.hash = '{fhash}' ORDER BY found ASC")
            data = [{"key": k[0], "json": k[1]} for k in self.sq_cur.fetchall()]

            self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {data[0]['key']}")
            stream_keys_0 = set([k[0] for k in self.sq_cur.fetchall()])
            json0 = data[0]["json"]

            print(f"Found {len(data)} keys for {r['hash']}")

            for k in data[1:]:
                if k["json"] != json0:
                    print(f"Found non-matching json for entry: {k['key']}", file=sys.stderr)
                    unequal_json += 1
                    continue

                self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {k['key']}")

                stream_keys = set([k[0] for k in self.sq_cur.fetchall()])
                if stream_keys == stream_keys_0:
                    equal_streams += 1
                    print(f"Found matching keys for entry: {k['key']}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {k['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {k['key']}")
                else:
                    print(f"Found non-matching keys for entry: {k['key']}", file=sys.stderr)
                    unequal_streams += 1

        print(f"Total Number of rows with duplicates: {nor}\n"
              f"Equal Streams: {equal_streams}\n"
              f"Unequal Streams: {unequal_streams}\n"
              f"Unequal Json: {unequal_json}\n")

    def remove_hash_table(self):
        self.debug_execute("DROP TABLE IF EXISTS hash_table")

    def perform_cleaning_of_metadata(self):
        self.debug_execute("SELECT COUNT(metadata.key), json FROM metadata GROUP BY json HAVING COUNT(metadata.key) > 1 ORDER BY found ASC")
        raw = self.sq_cur.fetchall()
        results = [{"count": row[0], "json": row[1].replace("'", "''")} for row in raw]

        nor = len(results)
        removed_entries = 0
        non_matching_parent = 0
        having_children = 0
        print(f"Found {nor} rows with duplicates")
        for r in results:
            self.debug_execute(f"SELECT key, parent FROM metadata WHERE json = '{r['json']}'")
            data = [{"key": k[0], "parent": k[1]} for k in self.sq_cur.fetchall()]

            parent_0 = data[0]["parent"]
            self.debug_execute(f"SELECT key FROM episodes WHERE parent = {data[0]['key']}")

            if self.sq_cur.fetchone() is None:
                print(f"Base entry {data[0]['key']} without children", file=sys.stderr)

            for d in data[1:]:
                if parent_0 != d['parent']:
                    non_matching_parent += 1
                    print(f"Non Matching Parent for entry: {d['key']}", file=sys.stderr)
                    continue

                # check if metadata has children.
                self.debug_execute(f"SELECT key FROM episodes WHERE parent = {d['key']}")
                if self.sq_cur.fetchone() is not None:
                    print(f"Found children for entry: {d['key']}", file=sys.stderr)
                    having_children += 1
                    continue

                removed_entries += 1
                self.debug_execute(f"DELETE FROM metadata WHERE key = {d['key']}")

        print(f"Searched {nor} rows with duplicates\n"
                f"Removed {removed_entries} entries\n"
                f"Found {non_matching_parent} non-matching parents\n$"
              f"Found {having_children} entries with children\n")




if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites_b64.db"
    obj = MiddlePruner(path)
    obj.perform_cleaning_of_metadata()
    # obj.create_hash_table()
    # obj.perform_cleaning_episodes()
    # obj.remove_hash_table()
    obj.sq_con.commit()