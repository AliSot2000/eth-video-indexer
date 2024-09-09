from eth_loader.base_sql import BaseSQliteDB
import json
from eth_loader.aux import from_b64, to_b64


class ConvertWrongB64(BaseSQliteDB):
    """
    Converter adds a has of the json data to the metadata and episodes table.
    Point being - I can detect possibly identical json based on hash
    """
    def __init__(self, db_path: str):
        super().__init__(db_path)

    def _generic_converter(self, tbl: str):
        """
        Convert the give table
        """
        self.debug_execute(f"SELECT key FROM {tbl} WHERE json NOT LIKE 'ey%'")
        keys = [entry[0] for entry in self.sq_cur.fetchall()]
        print(f"Found {len(keys)} keys in {tbl} to convert")

        for key in keys:
            self.debug_execute(f"SELECT json FROM {tbl} WHERE key = {key}")
            json_data = self.sq_cur.fetchone()[0]
            try:
                data = json.loads(json_data)
                json_str = to_b64(json.dumps(data, sort_keys=True))
                self.debug_execute(f"UPDATE {tbl} SET json = '{json_str}' WHERE key = {key}")
            except Exception as e:
                print(f"Error converting key {key}: {e}")
                print(f"Data: {json_data}")

    def convert(self):
        self._generic_converter("metadata")
        self._generic_converter("episodes")

if __name__ == "__main__":
    cwb64 = ConvertWrongB64("/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db")
    cwb64.convert()
    cwb64.sq_con.commit()
    cwb64.cleanup()