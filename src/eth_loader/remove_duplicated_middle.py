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

        deletes_overall = 0
        deletes_with_parent = 0

        unequal_json_parent = 0
        unequal_url_parent = 0
        unequal_parent_parent = 0

        unequal_parent = 0
        unequal_streams = 0
        unequal_json = 0
        nor = len(results)

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

            # Get the stream keys for the first row.
            self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {data[0]['key']}")

            # Create set of those keys
            stream_keys_0 = set([k[0] for k in self.sq_cur.fetchall()])

            # Create a copy of the first json.
            json0 = data[0]["json"]

            parent0 = data[0]["parent"]

            print(f"Found {len(data)} keys for hash: {r['hash']}")
            # Go through the remaining rows.
            for k in data[1:]:

                # Check the json of the row.
                if k["json"] != json0:
                    print(f"Found non-matching json for entry: {k['key']}, start_key: {data[0]['key']}",
                          file=sys.stderr)
                    unequal_json += 1
                    continue
                print(f"Json is a match for entry: {k['key']}, start_key: {data[0]['key']}")

                # Get the stream keys for the row.
                self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {k['key']}")

                # Create a set of those keys.
                stream_keys = set([k[0] for k in self.sq_cur.fetchall()])

                # check if the first and current set match
                if stream_keys != stream_keys_0:
                    # Inform about us having found a non-matching set of stream keys.
                    print(f"Found non-matching keys for entry: {k['key']} , start_key: {data[0]['key']}\n"
                          f"keys0: {stream_keys_0}\n"
                          f"keysn: {stream_keys}\n",
                          file=sys.stderr)
                    unequal_streams += 1
                    continue
                print(f"Streams are a match for entry: {k['key']}, start_key: {data[0]['key']}")

                if parent0 != k["parent"]:
                    unequal_parent += 1
                    print(f"Found non-matching parent for entry: {k['key']}, start_key: {data[0]['key']}\n"
                          f"parent0: {parent0}\n"
                          f"parentn: {k['parent']}\n"
                          f"Checking parents...",
                          file=sys.stderr)

                    # Get the parents and check them.
                    self.debug_execute(f"SELECT key, URL, parent, json FROM metadata "
                                       f"WHERE key IN ({k['parent']}, {parent0})")
                    parents = self.sq_cur.fetchall()
                    # Get the res dict.
                    res_dict = [{"key": r[0], "URL": r[1], "parent": r[2], "json": r[3]} for r in parents]

                    if len(parents) < 2:
                        assert len(parents) == 1, f"No Parent found. parentn: {k['parent']}, parent0: {parent0}"
                        if res_dict[0]["key"] == parent0:
                            print(f"Parent not found for entry: {k['key']}, start_key: {data[0]['key']}\n"
                                  f"parent0: {parent0}\n"
                                  f"parentn, not found: {k['parent']}\n"
                                  f"Deleting entry: {k['key']}\n")
                            self.debug_execute(f"DELETE FROM metadata WHERE key = {k['parent']}")
                            self.debug_execute(f"DELETE FROM episodes WHERE key = {k['key']}")
                            self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {k['key']}")
                            deletes_overall += 1
                            deletes_with_parent += 1
                            continue

                        else:
                            raise ValueError("Parent not found for parent0")

                    if res_dict[0]["json"] != res_dict[1]["json"]:
                        print(f"Parent json is not equal for entry: {k['key']}, start_key: {data[0]['key']}\n"
                              f"parent0: {res_dict[0]['json']}\n"
                              f"parentn: {res_dict[1]['json']}\n",
                              file=sys.stderr)
                        unequal_json_parent += 1
                        continue

                    if res_dict[0]["URL"] != res_dict[1]["URL"]:
                        print(f"Parent URL is not equal for entry: {k['key']}, start_key: {data[0]['key']}\n"
                              f"parent0: {res_dict[0]['URL']}\n"
                              f"parentn: {res_dict[1]['URL']}\n",
                              file=sys.stderr)
                        unequal_url_parent += 1
                        continue

                    if res_dict[0]["parent"] != res_dict[1]["parent"]:
                        print(f"Parent parent is not equal for entry: {k['key']}, start_key: {data[0]['key']}\n"
                              f"parent0: {res_dict[0]['parent']}\n"
                              f"parentn: {res_dict[1]['parent']}\n",
                              file=sys.stderr)
                        unequal_parent_parent += 1
                        continue

                    print(f"Parents are a match for entry: {k['key']}, start_key: {data[0]['key']},"
                          f" parent share same json, url and parent")
                    deletes_overall += 1
                    deletes_with_parent += 1
                    self.debug_execute(f"DELETE FROM metadata WHERE key = {k['parent']}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {k['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {k['key']}")
                    continue

                # Match found, remove duplicate (since we're ordering by order of appearance)
                deletes_overall += 1
                print(f"Found matching keys, parent and json for entry: {k['key']}, start_key: {data[0]['key']}")
                self.debug_execute(f"DELETE FROM episodes WHERE key = {k['key']}")
                self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {k['key']}")

        print(f"Searched {nor} rows with duplicates\n"
              f"Removed {deletes_overall} entries\n"
              f"Removed {deletes_with_parent} including parent\n"
              f"{'-' * 120}\n"
              f"Found {unequal_json} entries with non-matching json\n"
              f"Found {unequal_streams} entries with non-matching streams\n"
              f"Found {unequal_parent} entries with non-matching parent:\n"
              f"Found {unequal_json_parent} mismatches in parent json\n"
              f"Found {unequal_url_parent} mismatches in parent url\n"
              f"Found {unequal_parent_parent} mismatches in parent of parent\n")

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
# Dedup episodes

# Searched 9179 rows with duplicates
# Removed 12218 entries
# Removed 12182 including parent
# ------------------------------------------------------------------------------------------------------------------------
# Found 0 entries with non-matching json
# Found 0 entries with non-matching streams
# Found 15049 entries with non-matching parent:
# Found 2838 mismatches in parent json
# Found 29 mismatches in parent url
# Found 0 mismatches in parent of parent

# Dedup metadata
# Searched 432 rows with duplicates
# Removed 402 entries
# Found 8 non-matching parents
# Found 70 entries with children


# ITERATION 2
# Dedup episodes

# Searched 2560 rows with duplicates
# Removed 322 entries
# Removed 322 including parent
# ------------------------------------------------------------------------------------------------------------------------
# Found 0 entries with non-matching json
# Found 0 entries with non-matching streams
# Found 2867 entries with non-matching parent:
# Found 2516 mismatches in parent json
# Found 29 mismatches in parent url
# Found 0 mismatches in parent of parent

# Dedup metadata
# Searched 54 rows with duplicates
# Removed 0 entries
# Found 8 non-matching parents
# Found 70 entries with children
