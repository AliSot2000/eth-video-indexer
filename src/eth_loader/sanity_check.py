from eth_loader.base_sql import BaseSQliteDB
import logging
from typing import List


class SanityCheck(BaseSQliteDB):
    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.logger = logging.getLogger("sanity_checker")

    def _perform_check(self, stmt: str, on_success: str, on_failure: str, preamble: List[str] = None, epilogue: List[str] = None):
        """
        Perform a single sanity check.

        :param preamble: List of statements to execute before the main statement
        :param stmt: The main statement to execute
        :param epilogue: List of statements to execute after the main statement
        :param on_success: Message to print if the check is successful
        :param on_failure: Message to print if the check fails
        """
        # Execute the preamble
        if preamble is not None:
            for p in preamble:
                self.debug_execute(p)

        self.debug_execute(stmt)
        rows = self.sq_cur.fetchall()

        # Handling case when no offending records are found
        if len(rows) == 0:
            self.logger.info(on_success)

        # Handling case when offending records were found.
        else:
            self.logger.warning(on_failure)
            self.logger.debug("Printing the offending records")
            for row in rows:
                self.logger.debug(row)

        # Execute the epilogue
        if epilogue is not None:
            for e in epilogue:
                self.debug_execute(e)

    def check_all(self):
        """
        Run all the checks
        """
        self.check_site_table()
        self.check_metadata_table()
        self.check_episode_table()
        self.check_stream_table()

        self.check_metadata_episode_assoz_table()
        self.check_episode_stream_assoz_table()

    def check_site_table(self):
        """
        Check the site table for dangling records
        """
        # Checking for parents who are videos
        self.debug_execute("SELECT COUNT(key) FROM sites WHERE parent IN (SELECT key FROM sites WHERE IS_VIDEO = 1)")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {cnt} mal-attributed records in the site table (parent is a video)")

            self.logger.debug("Printing the offending records")

            self.debug_execute("SELECT * FROM sites WHERE parent IN (SELECT key FROM sites WHERE IS_VIDEO = 1)")
            for row in self.sq_cur.fetchall():
                self.logger.debug(row)
        else:
            self.logger.info("No mal-attributed records in the site table")

        # Checking for entries without parents.
        self.debug_execute("SELECT COUNT(key) FROM sites WHERE parent IS NULL")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {cnt} records who orphaned")

            self.logger.debug("Printing the offending records")

            self.debug_execute("SELECT * FROM sites WHERE parent IS NULL")
            for row in self.sq_cur.fetchall():
                self.logger.debug(row)
        else:
            self.logger.info("No orphaned records found.")


    def check_metadata_table(self):
        """
        Check the metadata table for dangling records
        """
        self.logger.info("No Sanity Checks for metadata table")

    def check_episode_table(self):
        """
        Check the episode table for dangling records
        """
        self.logger.info("No Sanity Checks for episode table")

    def check_stream_table(self):
        """
        Check the stream table for dangling records
        """
        self.logger.info("No Sanity Checks for stream table")

    def check_metadata_episode_assoz_table(self):
        """
        Check the assoz table for dangling records
        """
        self.debug_execute("SELECT COUNT(key) FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.episode_key NOT IN (SELECT key FROM episodes)")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {cnt} dangling episode keys in metadata_episode_assoz")
        else:
            self.logger.info(f"Found no dangling episode keys in metadata_episode_assoz")

        # Search based on metadata_key
        self.debug_execute("SELECT COUNT(key) FROM metadata_episode_assoz "
                           "WHERE metadata_episode_assoz.metadata_key NOT IN (SELECT key FROM metadata)")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {cnt} dangling metadata keys in metadata_episode_assoz")
        else:
            self.logger.info(f"Found no dangling metadata keys in metadata_episode_assoz")

    def check_episode_stream_assoz_table(self):
        """
        Check the assoz table for dangling records
        """
        self.debug_execute("SELECT COUNT(key) FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.stream_key NOT IN (SELECT key FROM streams)")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {self.sq_cur.fetchone()[0]} dangling stream keys in episode_stream_assoz")
        else:
            self.logger.info(f"Found no dangling stream keys in episode_stream_assoz")

        # Search based on episode_key
        self.debug_execute("SELECT COUNT(key) FROM episode_stream_assoz "
                           "WHERE episode_stream_assoz.episode_key NOT IN (SELECT key FROM episodes)")
        cnt = self.sq_cur.fetchone()[0]
        if cnt > 0:
            self.logger.warning(f"Found {self.sq_cur.fetchone()[0]} dangling episode keys in episode_stream_assoz")
        else:
            self.logger.info(f"Found no dangling episode keys in episode_stream_assoz")

    # TODO
    #   - last_seen is not Null

if __name__ == "__main__":
    l = logging.getLogger("sanity_checker")
    l.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    h.setLevel(logging.DEBUG)
    l.addHandler(h)
    l.propagate = False
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"

    sc = SanityCheck(path)
    sc.check_all()