from eth_loader.base_sql import BaseSQliteDB
import json
from eth_loader.aux import from_b64


class AddJsonHash(BaseSQliteDB):
    """
    Converter adds a has of the json data to the metadata and episodes table.
    Point being - I can detect possibly identical json based on hash
    """
    def __init__(self, db_path: str, b64: bool = False):
        super().__init__(db_path)
        self.b64 = b64

    def convert(self):
        """
        Convert the tables
        """
        try:
            self.prepare_tables()
        except Exception as e:
            print(f"Error preparing tables: {e}")

        self.populate_metadata_hash()
        self.populate_episodes_hash()

    def prepare_tables(self):
        """
        Add the columns to the tables
        """
        print("Adding column to metadata")
        self.debug_execute("ALTER TABLE metadata ADD COLUMN json_hash TEXT")

        print("Adding column to episodes")
        self.debug_execute("ALTER TABLE episodes ADD COLUMN json_hash TEXT")

    def populate_metadata_hash(self):
        """
        Populate the hash columns of the metadata table
        """
        self._populate_hash("metadata")

    def populate_episodes_hash(self):
        """
        Populate the hash columns of the episodes table
        """
        self._populate_hash("episodes")

    def _populate_hash(self, tbl: str):
        """
        Populate the hash columns
        """
        self.debug_execute(f"SELECT key, json FROM {tbl} WHERE json_hash is NULL")
        res = self.sq_cur.fetchone()

        while res:
            # print(f"Processing {tbl} key {res[0]}", flush=True, end="\r")
            print(f"Processing {tbl} key {res[0]}")
            key, json_data = res
            json_string = from_b64(json_data) if self.b64 else json_data
            try:
                sorted_string = json.dumps(json.loads(json_string), sort_keys=True)
            except json.JSONDecodeError:
                print(f"Error decoding {tbl} key {key} json {json_string}")
                sorted_string = json_string

            json_hash = hash(sorted_string)
            self.debug_execute(f"UPDATE {tbl} SET json_hash = '{json_hash}' WHERE key = {key}")

            self.debug_execute(f"SELECT key, json FROM {tbl} WHERE json_hash is NULL")
            res = self.sq_cur.fetchone()


if __name__ == "__main__":
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_hash.db"
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_hash_b64.db"

    ajh = AddJsonHash(path, b64=True)
    ajh.convert()
    ajh.sq_con.commit()
    ajh.cleanup()

