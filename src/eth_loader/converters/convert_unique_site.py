from eth_loader.base_sql import BaseSQliteDB


class ConvertSiteTable(BaseSQliteDB):
    def __init__(self, db_path: str):
        super().__init__(db_path)

        self.create_new_site_table()
        self.copy_content()
        self.drop_all_old_entities()
        self.move_new_table()
        self.recreate_index()
        print("Done with conversion")

    def create_new_site_table(self):
        """
        Create the new site table as a temp table
        """
        self.debug_execute("DROP TABLE IF EXISTS sites_temp")

        self.debug_execute("CREATE TABLE sites_temp "
                            "(key INTEGER PRIMARY KEY AUTOINCREMENT, "
                            "parent INTEGER, "
                            "URL TEXT, "
                            "IS_VIDEO INTEGER CHECK (IS_VIDEO IN (0, 1)),"
                            "found TEXT,"
                            "last_seen TEXT, UNIQUE (URL, IS_VIDEO));")  # found now is equivalent to 'last seen'

    def recreate_index(self):
        """
        Recreate the indexes on the new table
        """
        self.debug_execute("CREATE INDEX site_key_index ON sites (key)")
        # self.debug_execute("CREATE INDEX IF NOT EXISTS site_url_index ON sites (URL)")
        self.debug_execute("CREATE INDEX site_url_index ON sites (URL)")
        # self.debug_execute("CREATE INDEX IF NOT EXISTS site_parent_index ON sites (parent)")
        self.debug_execute("CREATE INDEX site_parent_index ON sites (parent)")

    def drop_all_old_entities(self):
        """
        Drop all the old entities (old sites table + old indexes)
        """

        self.debug_execute("DROP TABLE IF EXISTS sites")
        self.debug_execute("DROP INDEX IF EXISTS site_key_index")
        self.debug_execute("DROP INDEX IF EXISTS site_url_index")
        self.debug_execute("DROP INDEX IF EXISTS site_parent_index")

    def move_new_table(self):
        """
        Rename the new table so it replaces the old table
        """
        self.debug_execute("ALTER TABLE sites_temp RENAME TO sites")

    def copy_content(self):
        """
        Copy the content of the old table to the new table
        """
        self.debug_execute("INSERT INTO sites_temp (key, parent, URL, IS_VIDEO, found, last_seen)"
                           " SELECT key, parent, URL, IS_VIDEO, found, last_seen FROM sites")


if __name__ == "__main__":
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"

    c = ConvertSiteTable(path)
    c.sq_con.commit()
    print("Done?")