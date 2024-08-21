from eth_loader.base_sql import BaseSQliteDB


class PopulateMEA(BaseSQliteDB):
    def __init__(self, path: str):
        super().__init__(path)


    def create_table(self):
        self.debug_execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata_episode_assoz'")
        if self.sq_cur.fetchone() is None:
            print("Creating metadata episode assoz table")
            self.debug_execute("CREATE TABLE metadata_episode_assoz "
                               "(key INTEGER PRIMARY KEY AUTOINCREMENT, "
                               "metadata_key INTEGER REFERENCES metadata(key), "
                               "episode_key INTEGER REFERENCES episodes(key), UNIQUE (metadata_key, episode_key))")
            self.debug_execute("CREATE INDEX mea_index_keys ON metadata_episode_assoz (metadata_key, episode_key)")

    def populate_table(self):
        self.debug_execute("SELECT key, parent FROM episodes")
        raw = self.sq_cur.fetchall()
        tuples = [{"episode_key": x[0], "metadata_key": x[1]} for x in raw]
        print(f"Number of entries in assoz table {len(tuples)}")

        for tpl in tuples:
            print(f"Inserting {tpl}", flush=True, end="\r")
            self.debug_execute(f"INSERT INTO metadata_episode_assoz (metadata_key, episode_key) "
                               f"VALUES ({tpl['metadata_key']}, {tpl['episode_key']})")

    def remove_parent(self):
        self.debug_execute("DROP INDEX IF EXISTS episodes_url_parent_index")
        self.debug_execute("ALTER TABLE episodes DROP COLUMN parent")


if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    pmea = PopulateMEA(path)
    pmea.create_table()
    pmea.populate_table()
    print(f"Removing Column parent")
    pmea.remove_parent()
    print(f"Main Thing done")
    pmea.sq_con.commit()