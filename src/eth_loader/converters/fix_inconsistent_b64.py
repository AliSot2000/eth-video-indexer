from eth_loader.base_sql import BaseSQliteDB
import json
from eth_loader.aux import from_b64, to_b64


"""
Some Final Records weren't converted to b64 encoded in one of the runs. This file is to fix this instead of 
converting the entire database again
"""

class FixJson(BaseSQliteDB):
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
        self._generic_converter("metadata")
        self._generic_converter("episodes")

    def _generic_converter(self, tbl: str):
        """
        Update the table and fix the metadata and episode entries which have not b64 encoded json
        """
        self.debug_execute(f"SELECT key FROM {tbl} WHERE json NOT LIKE 'ey%'")
        keys = [r[0] for r in self.sq_cur.fetchall()]

        for k in keys:
            print(f"Fixing key {k} of table {tbl}")
            self.debug_execute(f"SELECT json FROM {tbl} WHERE key = {k}")
            json_data = self.sq_cur.fetchone()[0]
            try:
                json.loads(json_data)
                new_row = to_b64(json_data)
            except json.JSONDecodeError:
                print(f"Failed to decode json for key {k}")
                continue

            self.debug_execute(f"UPDATE {tbl} SET json = '{new_row}' WHERE key = {k}")


if __name__ == "__main__":
    fj = FixJson("/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db")
    fj.convert()
    fj.cleanup()