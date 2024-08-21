import sys

from eth_loader.base_sql import BaseSQliteDB


# TODO pruning needs to take into account the full link structure. there have been some episodes that have moved in the
#  html tree

class MiddlePruner(BaseSQliteDB):
    def __init__(self, db: str):
        super().__init__(db)

    def create_hash_table(self):
        """
        Creates a table of hashes that can be used to identify duplicates from the episodes table.
        :return:
        """
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
        """
        Select duplicates based on hash in episode and
        :return:
        """
        self.debug_execute(
            "SELECT COUNT(episodes.key), parent, hash_table.hash FROM episodes "
            "JOIN hash_table ON episodes.key = hash_table.key GROUP BY hash_table.hash "
            "HAVING COUNT(episodes.key) > 1 ORDER BY episodes.key ASC")
        raw = self.sq_cur.fetchall()

        # Parse the results into a list of dictionaries.
        results = [{"count": row[0], "parent": row[1], "hash": row[2]} for row in raw]

        del_fron_episodes = 0
        del_from_metadata = 0

        met_unequal_json = 0
        met_unequal_url = 0
        met_unequal_parent = 0
        met_parent_missing = 0

        ep_unequal_parent = 0
        ep_unequal_streams = 0
        ep_unequal_json = 0
        nor = len(results)
        ep_ge_2_eps = 0

        # Go through all rows of hashes which have duplicates.
        for r in results:
            fhash = r["hash"]

            # Get all rows with the same hash.
            self.debug_execute(
                f"SELECT episodes.key, json, episodes.parent, episodes.found "
                f"FROM episodes JOIN hash_table ON episodes.key = hash_table.key "
                f"WHERE hash_table.hash = '{fhash}' ORDER BY found ASC")

            # Parse the rows into a list of dictionaries.
            data = [{"key": k[0], "json": k[1], "parent": k[2], "found": k[3]} for k in self.sq_cur.fetchall()]

            key0 = data[0]["key"]

            # Get the stream keys for the first row.
            self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {key0}")

            # Create set of those keys
            stream_keys_0 = set([k[0] for k in self.sq_cur.fetchall()])

            # Create a copy of the first json.
            json0 = data[0]["json"]

            parent0 = data[0]["parent"]

            # Keeping track of more multiples.
            if len(data) > 2:
                ep_ge_2_eps += 1

            # print(f"EPISODES: Found {len(data)} keys for hash: {r['hash']}")
            # Go through the remaining rows.
            for contender in data[1:]:

                # Check the json of the row.
                if contender["json"] != json0:
                    print(f"EPISODES: Found non-matching json for entry: {contender['key']}, "
                          f"start_key: {key0}",
                          file=sys.stderr)
                    ep_unequal_json += 1
                    continue
                # print(f"Json is a match for entry: {contender['key']}, start_key: {key0}")

                # Get the stream keys for the row.
                self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {contender['key']}")

                # Create a set of those keys.
                stream_keys = set([k[0] for k in self.sq_cur.fetchall()])

                # check if the first and current set match
                if stream_keys != stream_keys_0:
                    # Inform about us having found a non-matching set of stream keys.
                    print(f"EPISODES: Found non-matching keys for entry: {contender['key']}, "
                          f"start_key: {key0}\n"
                          f"keys0: {stream_keys_0}\n"
                          f"keysn: {stream_keys}\n",
                          file=sys.stderr)
                    ep_unequal_streams += 1
                    continue
                # print(f"Streams are a match for entry: {contender['key']}, start_key: {key0}")

                # Parents are a match, delete the contender from the episodes table.
                if parent0 == contender["parent"]:
                    # Match found, remove duplicate (since we're ordering by order of appearance)
                    del_fron_episodes += 1
                    # print(
                    #     f"Found matching keys, parent_id and json for entry: {contender['key']}, start_key: {key0}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {contender['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {contender['key']}")
                    continue

                if parent0 != k["parent"]:
                    print(f"Found non-matching parent for entry: {k['key']}, start_key: {data[0]['key']}\n"
                          f"parent0: {parent0}\n"
                          f"parentn: {k['parent']}\n"
                          f"Checking parents...",
                          file=sys.stderr)
                ep_unequal_parent += 1

                    # Get the parents and check them.
                    self.debug_execute(f"SELECT key, URL, parent, json FROM metadata "
                    parents = self.sq_cur.fetchall()
                    # Get the res dict.
                    res_dict = [{"key": r[0], "URL": r[1], "parent": r[2], "json": r[3]} for r in parents]
                                   f"WHERE key IN ({contender['parent']}, {parent0})")

                # Second parent is missing for some reason?
                if len(raw_parents) < 2:
                    assert len(raw_parents) == 1, f"No Parent found. parentn: {contender['parent']}, parent0: {parent0}"
                    if parent_dict[0]["key"] == parent0:
                        # print(f"METADATA: Parent not found for entry: {contender['key']}, start_key: {key0}\n"
                        #       f"parent0: {parent0}\n"
                        #       f"parentn, not found: {contender['parent']}\n"
                        #       f"Deleting entry: {contender['key']}\n")
                        # The first one should be redundant.
                        self.debug_execute(f"DELETE FROM metadata WHERE key = {contender['parent']}")
                        self.debug_execute(f"DELETE FROM episodes WHERE key = {contender['key']}")
                        self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {contender['key']}")
                        del_fron_episodes += 1
                        # del_from_metadata += 1
                        met_parent_missing += 1

                        else:
                            raise ValueError("Parent not found for parent0")

                if parent_dict[0]["json"] != parent_dict[1]["json"]:
                    # print(f"METADATA: json is not equal for entry: {contender['key']}, start_key: {key0}\n"
                          # f"parent0: {parent_dict[0]['json']}\n"
                          # f"parentn: {parent_dict[1]['json']}\n",
                          # file=sys.stderr)
                    met_unequal_json += 1
                    continue

                if parent_dict[0]["URL"] != parent_dict[1]["URL"]:
                    print(f"METADATA: URL is not equal for entry: {contender['key']}, start_key: {key0}\n"
                          f"parent0: {parent_dict[0]['URL']}\n"
                          f"parentn: {parent_dict[1]['URL']}\n",
                          file=sys.stderr)
                    met_unequal_url += 1

                if parent_dict[0]["parent"] != parent_dict[1]["parent"]:
                    print(f"METADATA: site parent is not equal for entry: {contender['key']}, start_key: {key0}\n"
                          f"parent0: {parent_dict[0]['parent']}\n"
                          f"parentn: {parent_dict[1]['parent']}\n",
                          file=sys.stderr)
                    met_unequal_parent += 1

                    print(f"Parents are a match for entry: {k['key']}, start_key: {data[0]['key']},"
                          f" parent share same json, url and parent")
                    self.debug_execute(f"DELETE FROM metadata WHERE key = {k['parent']}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {k['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {k['key']}")
                    continue
                del_fron_episodes += 1
                del_from_metadata += 1

        print(f"EPISODES: Searched {nor} rows with duplicates\n"
              f"EPISODES: More than one duplicate for {ep_ge_2_eps} rows\n"
              f"EPISODES: Removed {del_fron_episodes} entries\n"
              f"\n"
              f"EPISODES: Found {ep_unequal_json} entries with non-matching json\n"
              f"EPISODES: Found {ep_unequal_streams} entries with non-matching streams\n"
              f"EPISODES: Found {ep_unequal_parent} entries with non-matching parent\n"
              f"\n"
              f"METADATA: Removed {del_from_metadata} rows with duplicates\n"
              f"METADATA: Found {met_parent_missing} missing parents\n"
              f"METADATA: Found {met_unequal_json} mismatches in json\n"
              f"METADATA: Found {met_unequal_url} mismatches in url\n"
              f"METADATA: Found {met_unequal_parent} mismatches in parent_id\n")

    def remove_hash_table(self):
        """
        Removes the hash table
        :return:
        """
        self.debug_execute("DROP TABLE IF EXISTS hash_table")

    def perform_cleaning_of_metadata(self):
        """
        Clean the metadata table from duplicates. Detect duplicates based on parent and json.
        :return:
        """
        # Get all duplicate rows based on json.
        self.debug_execute("SELECT COUNT(metadata.key), json FROM metadata "
                           "GROUP BY json HAVING COUNT(metadata.key) > 1 ORDER BY found ASC")
        raw = self.sq_cur.fetchall()

        # Parse the results into a list of dictionaries.
        results = [{"count": row[0], "json": row[1].replace("'", "''")} for row in raw]

        nor = len(results)
        removed_entries = 0
        non_matching_parent = 0
        having_children = 0
        print(f"Found {nor} rows with duplicates")

        # Go through all rows of duplicates.
        for r in results:
            # Get all rows with the same json.
            self.debug_execute(f"SELECT key, parent FROM metadata WHERE json = '{r['json']}'")

            # Parse the rows into a list of dictionaries.
            data = [{"key": k[0], "parent": k[1]} for k in self.sq_cur.fetchall()]

            # Get the parent of the first row.
            parent_0 = data[0]["parent"]

            # Select all matching rows based on parent in episodes table
            self.debug_execute(f"SELECT key FROM episodes WHERE parent = {data[0]['key']}")

            if self.sq_cur.fetchone() is None:
                print(f"Base entry {data[0]['key']} without children", file=sys.stderr)

            # Go through all rows except the first one.
            for d in data[1:]:

                # Check if the parent of the row is the same as the first row.
                if parent_0 != d['parent']:
                    non_matching_parent += 1
                    print(f"Non Matching Parent for entry: {d['key']}, start_key: {data[0]['key']}\n"
                          f"parent0: {parent_0}\n"
                          f"parentn: {d['parent']}", file=sys.stderr)
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
              f"Found {non_matching_parent} non-matching parents\n"
              f"Found {having_children} entries with children\n")


# TODO check metadata for duplicates and ignore the lower end.
if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites_b64.db"
    obj = MiddlePruner(path)
    obj.create_hash_table()
    obj.perform_cleaning_episodes()
    obj.remove_hash_table()
    obj.perform_cleaning_of_metadata()
    obj.sq_con.commit()

# ITERATION +

# Stats on the 10.5.2024, after copying the files freshly from the backup.
# Freshly created from the original db
# b64 version
# Dedup episodes
# Found 2 keys for hash: 3077204854470579146
# Json is a match for entry: 237366, start_key: 180991
# Streams are a match for entry: 237366, start_key: 180991
# Searched 11 rows with duplicates
# Removed 0 entries
# Removed 0 including parent
# ------------------------------------------------------------------------------------------------------------------------
# Found 0 entries with non-matching json
# Found 0 entries with non-matching streams
# Found 11 entries with non-matching parent:
# Found 0 mismatches in parent json
# Found 11 mismatches in parent url
# Found 0 mismatches in parent of parent

# Dedup metadata
# Dedup episodes
# Searched 4 rows with duplicates
# Removed 0 entries
# Found 4 non-matching parents
# Found 0 entries with children

# => Constant with iterations

# non-b64 version
# Searched 8631 rows with duplicates
# Removed 6080 entries
# Removed 6062 including parent
# ------------------------------------------------------------------------------------------------------------------------
# Found 0 entries with non-matching json
# Found 0 entries with non-matching streams
# Found 8613 entries with non-matching parent:
# Found 2529 mismatches in parent json
# Found 22 mismatches in parent url
# Found 0 mismatches in parent of parent

# Dedup metadata
# Searched 428 rows with duplicates
# Removed 402 entries
# Found 4 non-matching parents
# Found 46 entries with children

# Iteration 2
# Dedup episodes
# Searched 2551 rows with duplicates
# Removed 314 entries
# Removed 314 including parent
# ------------------------------------------------------------------------------------------------------------------------
# Found 0 entries with non-matching json
# Found 0 entries with non-matching streams
# Found 2551 entries with non-matching parent:
# Found 2215 mismatches in parent json
# Found 22 mismatches in parent url
# Found 0 mismatches in parent of parent

# Dedup metadata
# Searched 50 rows with duplicates
# Removed 0 entries
# Found 4 non-matching parents
# Found 46 entries with children

# => Constant with iterations 2 and upwards
