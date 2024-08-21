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
            "SELECT COUNT(episodes.key), hash_table.hash FROM episodes "
            "JOIN hash_table ON episodes.key = hash_table.key GROUP BY hash_table.hash "
            "HAVING COUNT(episodes.key) > 1 ORDER BY episodes.key ASC")
        raw = self.sq_cur.fetchall()

        # Parse the results into a list of dictionaries.
        results = [{"count": row[0], "hash": row[1]} for row in raw]

        del_fron_episodes = 0

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
                f"SELECT episodes.key, json, episodes.found "
                f"FROM episodes JOIN hash_table ON episodes.key = hash_table.key "
                f"WHERE hash_table.hash = '{fhash}' ORDER BY found ASC")

            # Parse the rows into a list of dictionaries.
            data = [{"key": k[0], "json": k[1], "found": k[2]} for k in self.sq_cur.fetchall()]

            key0 = data[0]["key"]

            # Get the stream keys for the first row.
            self.debug_execute(f"SELECT stream_key FROM episode_stream_assoz WHERE episode_key = {key0}")

            # Create set of those keys
            stream_keys_0 = set([k[0] for k in self.sq_cur.fetchall()])

            # Create a copy of the first json.
            json0 = data[0]["json"]

            # Get the parents of the first row
            self.debug_execute(f"SELECT metadata_key FROM metadata_episode_assoz WHERE episode_key = {key0}")
            parent0 = set([k[0] for k in self.sq_cur.fetchall()])

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

                # get parents of contender.
                self.debug_execute(f"SELECT metadata_key "
                                   f"FROM metadata_episode_assoz "
                                   f"WHERE episode_key = {contender['key']}")
                contender["parent"] = set([k[0] for k in self.sq_cur.fetchall()])

                # Parents sets match, remove all entries to the contender
                if parent0 == contender["parent"]:
                    # Match found, remove duplicate (since we're ordering by order of appearance)
                    del_fron_episodes += 1
                    # print(
                    #     f"Found matching keys, parent_id and json for entry: {contender['key']}, start_key: {key0}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {contender['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {contender['key']}")
                    self.debug_execute(f"DELETE FROM metadata_episode_assoz WHERE episode_key = {contender['key']}")
                    continue

                else:
                    ep_unequal_parent += 1
                    print(f"EPISODES: Found non-matching parent for entry: {contender['key']}, "
                          f"start_key: {key0}\n"
                          f"parent0: {parent0}\n"
                          f"parentn: {contender['parent']}\n"
                          f"Changing Entries for contender to target",
                          file=sys.stderr)

                    children_to_establish = contender['parent'] - parent0
                    for child in children_to_establish:
                        self.debug_execute(f"INSERT OR IGNORE INTO metadata_episode_assoz (metadata_key, episode_key) "
                                           f"VALUES ({child}, {key0})")

                    del_fron_episodes += 1
                    print(
                        f"Updated parents, removed: {contender['key']}, start_key: {key0}")
                    self.debug_execute(f"DELETE FROM episodes WHERE key = {contender['key']}")
                    self.debug_execute(f"DELETE FROM episode_stream_assoz WHERE episode_key = {contender['key']}")
                    self.debug_execute(f"DELETE FROM metadata_episode_assoz WHERE episode_key = {contender['key']}")

        print(f"EPISODES: Searched {nor} rows with duplicates\n"
              f"EPISODES: More than one duplicate for {ep_ge_2_eps} rows\n"
              f"EPISODES: Removed {del_fron_episodes} entries\n"
              f"\n"
              f"EPISODES: Found {ep_unequal_json} entries with non-matching json\n"
              f"EPISODES: Found {ep_unequal_streams} entries with non-matching streams\n"
              f"EPISODES: Found {ep_unequal_parent} entries with non-matching parent\n")

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
            self.debug_execute(f"SELECT key, parent FROM metadata WHERE json = '{r['json']}' ORDER BY found ASC")

            # Parse the rows into a list of dictionaries.
            data = [{"key": k[0], "parent": k[1]} for k in self.sq_cur.fetchall()]

            key0 = data[0]["key"]

            # Get the parent of the first row.
            parent_0 = data[0]["parent"]

            # Select all matching rows based on parent in episodes table
            self.debug_execute(f"SELECT episode_key FROM metadata_episode_assoz WHERE metadata_key = {key0}")

            children0 = self.sq_cur.fetchall()
            if len(children0) == 0:
                # very common occurrence need to think about that.
                print(f"Base entry {key0} without children", file=sys.stderr)

            # Go through all rows except the first one.
            for d in data[1:]:

                # Check if the parent of the row is the same as the first row.
                if parent_0 != d['parent']:
                    non_matching_parent += 1
                    print(f"Non Matching Parent for entry: {d['key']}, start_key: {key0}\n"
                          f"parent0: {parent_0}\n"
                          f"parentn: {d['parent']}", file=sys.stderr)
                    continue

                # check if metadata has children.
                self.debug_execute(f"SELECT episode_key FROM metadata_episode_assoz WHERE metadata_key = {d['key']}")
                children = self.sq_cur.fetchall()
                if len(children) > 0:
                    print(f"Found children for entry: {d['key']}")
                    # print(f"Found children for entry: {d['key']}\n"
                    #       f"children: {children}\n"
                    #       f"updating...", file=sys.stderr)
                    having_children += 1
                    self.debug_execute(f"DELETE FROM  metadata_episode_assoz "
                                       f"WHERE metadata_key = {d['key']}")
                    for child in children:
                        self.debug_execute(f"INSERT OR IGNORE INTO metadata_episode_assoz (metadata_key, episode_key) "
                                           f"VALUES ({key0}, {child[0]})")

                # self.debug_execute(f"SELECT episode_key FROM metadata_episode_assoz WHERE metadata_key = {d['key']}")
                # print(f"New children for entry: {d['key']}\n"
                #       f"children: {self.sq_cur.fetchall()}\n")
                #
                # self.debug_execute(f"SELECT episode_key FROM metadata_episode_assoz WHERE metadata_key = {key0}")
                # print(f"New Children of Base Entry {key0}:\n"
                #       f"children: {self.sq_cur.fetchall()}")

                removed_entries += 1
                self.debug_execute(f"DELETE FROM metadata WHERE key = {d['key']}")

        print(f"Searched {nor} rows with duplicates\n"
              f"Removed {removed_entries} entries\n"
              f"Found {non_matching_parent} non-matching parents\n"
              f"Found {having_children} entries with children\n")

    def remove_dangling_assoz(self):
        """
        Drop the assoz table entries that have either a missing episode or metadata entry.
        """
        # Search based on stream_key
        self.debug_execute("SELECT COUNT(key) FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.stream_key NOT IN (SELECT key FROM streams)")

        print(f"Found {self.sq_cur.fetchone()[0]} dangling stream keys in episode_stream_assoz")
        self.debug_execute("DELETE FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.stream_key NOT IN (SELECT key FROM streams)")

        # Search based on episode_key
        self.debug_execute("SELECT COUNT(key) FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.episode_key NOT IN (SELECT key FROM episodes)")
        print(f"Found {self.sq_cur.fetchone()[0]} dangling episode keys in episode_stream_assoz")
        self.debug_execute("DELETE FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.episode_key NOT IN (SELECT key FROM episodes)")

        # Search based on episode_key
        self.debug_execute("SELECT COUNT(key) FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.episode_key NOT IN (SELECT key FROM episodes)")
        print(f"Found {self.sq_cur.fetchone()[0]} dangling episode keys in metadata_episode_assoz")

        self.debug_execute("DELETE FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.episode_key NOT IN (SELECT key FROM episodes)")

        # Search based on metadata_key
        self.debug_execute("SELECT COUNT(key) FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.metadata_key NOT IN (SELECT key FROM metadata)")
        print(f"Found {self.sq_cur.fetchone()[0]} dangling metadata keys in metadata_episode_assoz")
        self.debug_execute("DELETE FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.metadata_key NOT IN (SELECT key FROM metadata)")


# TODO check metadata for duplicates and ignore the lower end.
if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    obj = MiddlePruner(path)
    obj.create_hash_table()
    obj.perform_cleaning_episodes()
    obj.remove_hash_table()
    obj.perform_cleaning_of_metadata()
    obj.remove_dangling_assoz()

    obj.sq_con.commit()
