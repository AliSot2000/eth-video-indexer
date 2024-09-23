from eth_loader.base_sql import BaseSQliteDB
import json

from eth_loader.aux import from_b64


class AddNonJsonRecordType(BaseSQliteDB):
    def __init__(self, db_path: str, b64: bool):
        super().__init__(db_path)
        self.b64 = b64

    def convert(self):
        self._generic_converter("metadata")
        self._generic_converter("episodes")

    def _generic_converter(self, tbl: str):
        """
        Generically convert a table to the new record_type_format
        - create new column for record_type with 0, 1, 2, 3
        - populate the column
        - remove old record_type column
        - move the record_type_new column to record_type_column
        """
        # Step 1 add new column
        self.debug_execute(f"ALTER TABLE {tbl} ADD COLUMN record_type_new "
                           f"INTEGER CHECK (record_type_new IN (0, 1, 2, 3))")

        # Step 2, create the new column
        self.debug_execute(f"SELECT key, json, record_type FROM {tbl} ORDER BY key ASC LIMIT 100")
        results = [{"key": res[0], "json": res[1], "record_type": res[2]} for res in self.sq_cur.fetchall()]
        key = results[-1]["key"]

        while key is not None:
            # process the rows
            for row in results:
                print(f"Processing Key: {row['key']:06}")
                try:
                    tgt_json = from_b64(row['json']) if self.b64 else row['json']
                    _ = json.loads(tgt_json)
                    # Copy the value of the record_type, since we're having valid json
                    self.debug_execute(f"UPDATE {tbl} SET record_type_new = {row['record_type']}"
                                       f" WHERE key = {row['key']}")
                except json.JSONDecodeError:
                    # Error, we're marking it as a non_jons type
                    print(f"Found non_json json field, key: {row['key']}, json: {row['json'][:120]}")
                    self.debug_execute(f"UPDATE {tbl} SET record_type_new = 3 WHERE key = {row['key']}")

            self.debug_execute(f"SELECT key, json, record_type FROM {tbl} WHERE key > {key} ORDER BY key ASC LIMIT 100")
            results = [{"key": res[0], "json": res[1], "record_type": res[2]} for res in self.sq_cur.fetchall()]

            # Update the key
            key = results[-1]["key"] if len(results) > 0 else None

        # Step 3 remove the old column
        self.debug_execute(f"ALTER TABLE {tbl} DROP COLUMN record_type")

        # Step 4, move the new column
        self.debug_execute(f"ALTER TABLE {tbl} RENAME record_type_new TO record_type")


if __name__ == "__main__":
    # b64, p1 = False, "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    b64, p1 = True, "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    obj = AddNonJsonRecordType(db_path=p1, b64=b64)
    obj.convert()
    obj.cleanup()