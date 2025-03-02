import logging
import sys
from typing import List

from eth_loader.base_sql import BaseSQliteDB


class SanityCheck(BaseSQliteDB):
    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.logger = logging.getLogger("sanity_checker")

    def _perform_check(self,
                       stmt: str,
                       on_success: str,
                       on_failure: str,
                       preamble: List[str] = None,
                       epilogue: List[str] = None) -> bool:
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
            res = False

        # Handling case when offending records were found.
        else:
            res = True
            self.logger.warning(on_failure)
            self.logger.debug(f"Printing the {len(rows)} offending records")
            for row in rows:
                self.logger.debug(row)

        # Execute the epilogue
        if epilogue is not None:
            for e in epilogue:
                self.debug_execute(e)

        return res

    def check_all(self) -> bool:
        """
        Run all the checks
        """
        res = self.check_site_table()
        res = res or self.check_metadata_table()
        res = res or self.check_episode_table()
        res = res or self.check_stream_table()

        res = res or self.check_metadata_episode_assoz_table()
        res = res or self.check_episode_stream_assoz_table()
        return res

    # ==================================================================================================================
    # Checks for the Site Table
    # ==================================================================================================================

    def check_site_table(self) -> bool:
        """
        Check the site table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Site Table")
        self.logger.info("=" * 120)

        res = False

        # Check for parent is video
        res = res or self._perform_check(
            stmt="SELECT * FROM sites WHERE parent IN (SELECT key FROM sites WHERE IS_VIDEO = 1)",
            on_failure="Found mal-attributed records in the site table (parent is a video)",
            on_success="No mal-attributed records in the site table")

        # Checking for entries without parents.
        res = res or self._perform_check(stmt="SELECT * FROM sites WHERE parent IS NULL",
                                         on_failure="Found orphaned records in site table",
                                         on_success="No orphaned records in site table")

        # Checking for entries without a last_seen field.
        res = res or self._perform_check(stmt="SELECT * FROM sites WHERE last_seen IS NULL",
                                         on_failure="Found records with empty last_seen field in site table",
                                         on_success="No records with empty last_seen field found in site table")

        # Checking for entries without a found field.
        res = res or self._perform_check(stmt="SELECT * FROM sites WHERE found IS NULL",
                                         on_failure="Found records with empty found field in site table",
                                         on_success="No records with empty found field found in site table")

        self.logger.info("Done with Site Table Checks")
        return res

    # ==================================================================================================================
    # Checks for the Metadata Table
    # ==================================================================================================================

    def check_metadata_table(self):
        """
        Check the metadata table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Metadata Table")
        self.logger.info("=" * 120)

        res = False

        # Checking for entries without a last_seen field.
        res = res or self._perform_check(stmt="SELECT * FROM metadata WHERE last_seen IS NULL",
                                         on_failure="Found records with empty last_seen field in metadata table",
                                         on_success="No records with empty last_seen field found in metadata table")

        # Check no initial
        res = res or self._perform_check(stmt="SELECT url "
                                              "FROM metadata "
                                              "WHERE record_type IN (0, 1, 2) " # INFO need to exclude non-json records
                                              "GROUP BY URL HAVING COUNT(*) > 1 "
                                              "AND SUM(CASE metadata.record_type WHEN 0 THEN 1 ELSE 0 END) = 0;",
                                         on_success="No urls with no initial record found in metadata",
                                         on_failure="Found urls with no initial record in metadata")

        # more than one initial
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM metadata "
                 "GROUP BY URL HAVING SUM (CASE metadata.record_type WHEN 0 THEN 1 ELSE 0 END) > 1;",
            on_success="No urls with more than one initial record found in metadata",
            on_failure="Found urls with more than one initial record in metadata")

        # Diff but no final
        res = res or  self._perform_check(
            stmt="SELECT url "
                 "FROM metadata "
                 "GROUP BY URL "
                 "HAVING SUM (CASE metadata.record_type WHEN 1 THEN 1 ELSE 0 END) > 0 " # Has diff
                 "AND SUM (CASE metadata.record_type WHEN 2 THEN 1 ELSE 0 END) = 0;",
            on_success="No urls with diff and no final record found in metadata",
            on_failure="Found urls with diff and no final record in metadata")

        # more than one final
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM metadata "
                 "GROUP BY URL HAVING SUM (CASE metadata.record_type WHEN 2 THEN 1 ELSE 0 END) > 1;",
            on_success="No urls with more than one final record found in metadata",
            on_failure="Found urls with more than one final record in metadata")

        # no diff but more than two values per url
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM metadata "
                 "GROUP BY URL HAVING COUNT(*) > 1 "
                 "AND SUM (CASE metadata.record_type WHEN 1 THEN 1 ELSE 0 END) = 0;",
            on_success="No urls with no diff but more than one records found in metadata",
            on_failure="Found urls with no diff but more than one records in metadata")

        # check final records aren't found
        res = res or self._perform_check(
            stmt="SELECT key FROM metadata WHERE record_type = 2 AND found IS NOT NULL;",
            on_success="All final records have no found date in metadata table",
            on_failure="Found final records with a found date in metadata table")

        # if there's exactly one entry for a given url, the type is initial not final
        res = res or self._perform_check(
            stmt="SELECT URL FROM metadata WHERE record_type IN (1, 2) GROUP BY URL HAVING COUNT(*) = 1;",
            on_success="All single URLS have an initial record in metadata table",
            on_failure="Found single URLS have no initial record in metadata table")

        # Check the non-deprecated records are either initial records or the newest diff record + the final record
        res = res or self._perform_check(
            preamble=[
            # Cleanup - drop the temp table if it exists
            "DROP TABLE IF EXISTS temp;",

            # Get the correctly attributed records from diff records
            "CREATE TABLE temp AS "
            "WITH LatestRecord AS (SELECT URL, parent, MAX(DATETIME(found)) AS "
            "                      latest_found FROM metadata WHERE record_type = 1 "
            "                      GROUP BY URL, parent ) "
            "SELECT t.key FROM metadata t "
            "JOIN LatestRecord lr ON t.URL = lr.URL "
            "                     AND t.found = lr.latest_found "
            "                     AND t.parent = lr.parent "
            "WHERE t.record_type = 1 AND deprecated = 0",

            # Get the correctly attributed records initial records
            "INSERT INTO temp SELECT key FROM metadata "
            "WHERE deprecated = 0 "
            "AND record_type = 0 "
            "AND key IN (SELECT key FROM metadata GROUP BY parent, URL HAVING COUNT(*) = 1)",

            # Get the correctly attributed final records
            "INSERT INTO temp SELECT key FROM metadata "
            "WHERE deprecated = 0 "
            "AND record_type = 2 "
            "AND URL IN (SELECT URL FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2) "
            "AND parent IN (SELECT parent FROM metadata GROUP BY parent, URL HAVING COUNT(*) > 2)",
                
            # Get correctly attributed non-json records
            "INSERT INTO temp SELECT key FROM metadata "
            "WHERE deprecated = 0 "
            "AND record_type = 3 "
            "AND URL IN (SELECT URL FROM metadata WHERE deprecated = 0 GROUP BY parent, URL HAVING COUNT(*) = 1) "
            "AND parent IN (SELECT parent FROM metadata WHERE deprecated = 0 GROUP BY parent, URL HAVING COUNT(*) = 1)"
        ],
            stmt="SELECT URL, parent FROM metadata WHERE deprecated = 0 "
                 "                                 AND key NOT IN (SELECT key FROM temp) ",
            on_success="All deprecated entries are either singular initial or final and diff, with the "
                       "diff being the newest diff record in metadata table",
            on_failure="Found Entries that were not deprecated but not singular initial or final and diff, "
                       "with the diff being the newest diff record in metadata table",
            epilogue=["DROP TABLE IF EXISTS temp"]
        )

        self.logger.info("Done with Metadata Table Checks")
        return res

    # ==================================================================================================================
    # Checks for the Episode Table
    # ==================================================================================================================

    def check_episode_table(self):
        """
        Check the episode table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Episodes Table")
        self.logger.info("=" * 120)

        res = False

        # Checking for entries without a last_seen field.
        res = res or self._perform_check(stmt="SELECT * FROM episodes WHERE last_seen IS NULL",
                                         on_failure="Found records with empty last_seen field in episodes table",
                                         on_success="No records with empty last_seen field found in episodes table")

        # Check no initial
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM episodes "
                 "WHERE record_type IN (0, 1, 2) "  # INFO need to exclude non-json records
                 "GROUP BY URL HAVING COUNT(*) > 1 "
                 "AND SUM(CASE episodes.record_type WHEN 0 THEN 1 ELSE 0 END) = 0;",
            on_success="No urls with no initial record found in episodes",
            on_failure="Found urls with no initial record in episodes")

        # more than one initial
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM episodes "
                 "GROUP BY URL HAVING SUM (CASE episodes.record_type WHEN 0 THEN 1 ELSE 0 END) > 1;",
            on_success="No urls with more than one initial record found in episodes",
            on_failure="Found urls with more than one initial record in episodes")

        # Diff but no final
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM episodes "
                 "GROUP BY URL "
                 "HAVING SUM (CASE episodes.record_type WHEN 1 THEN 1 ELSE 0 END) > 0 "  # Has diff
                 "AND SUM (CASE episodes.record_type WHEN 2 THEN 1 ELSE 0 END) = 0;",
            on_success="No urls with diff and no final record found in episodes",
            on_failure="Found urls with diff and no final record in episodes")

        # more than one final
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM episodes "
                 "GROUP BY URL HAVING SUM (CASE episodes.record_type WHEN 2 THEN 1 ELSE 0 END) > 1;",
            on_success="No urls with more than one final record found in episodes",
            on_failure="Found urls with more than one final record in episodes")

        # no diff but more than two values per url
        res = res or self._perform_check(
            stmt="SELECT url "
                 "FROM episodes "
                 "GROUP BY URL HAVING COUNT(*) > 1 "
                 "AND SUM (CASE episodes.record_type WHEN 1 THEN 1 ELSE 0 END) = 0;",
            on_success="No urls with no diff but more than one records found in episodes",
            on_failure="Found urls with no diff but more than one records in episodes")

        # check final records aren't found
        res = res or self._perform_check(
            stmt="SELECT key FROM episodes WHERE record_type = 2 AND found IS NOT NULL;",
            on_success="All final records have no found date in episodes table",
            on_failure="Found final records with a found date in episodes table")

        # if there's exactly one json entry for a given url, the type is initial not final
        res = res or self._perform_check(
            stmt="SELECT URL FROM episodes WHERE record_type IN (1, 2) GROUP BY URL HAVING COUNT(*) = 1;",
            on_success="All single URLS have an initial record in episodes table",
            on_failure="Found single URLS have no initial record in episodes table")

        # Check the non-deprecated records are either initial records or the newest diff record + the final record
        res = res or self._perform_check(
            preamble=[
                # Cleanup - drop the temp table if it exists
                "DROP TABLE IF EXISTS temp",

                # Get the correctly attributed records from diff records
                "CREATE TABLE temp AS "
                "WITH LatestRecord AS (SELECT URL, MAX(DATETIME(found)) AS "
                "                      latest_found FROM episodes WHERE record_type = 1 "
                "                      GROUP BY URL ) "
                "SELECT t.key FROM episodes t "
                "JOIN LatestRecord lr ON t.URL = lr.URL "
                "                     AND t.found = lr.latest_found "
                "WHERE t.record_type = 1 AND deprecated = 0",

                # Get the correctly attributed records initial records
                "INSERT INTO temp SELECT key FROM episodes "
                "WHERE deprecated = 0 "
                "AND record_type = 0 "
                "AND key IN (SELECT key FROM episodes GROUP BY URL HAVING COUNT(*) = 1)",

                # Get the correctly attributed final records
                "INSERT INTO temp SELECT key FROM episodes "
                "WHERE deprecated = 0 "
                "AND record_type = 2 "
                "AND URL IN (SELECT URL FROM episodes GROUP BY URL HAVING COUNT(*) > 2) ",

                # Get correctly attributed non-json records
                "INSERT INTO temp SELECT key FROM episodes "
                "WHERE deprecated = 0 "
                "AND record_type = 3 "
                # INFO: We're only trying to capture the correctly attributed records, so the ones which have exactly 
                #   one entry non-deprecated record for a given non-json entry.
                "AND URL IN (SELECT URL FROM episodes WHERE deprecated = 0 GROUP BY URL HAVING COUNT(*) = 1) "
            ],
            stmt="SELECT URL FROM episodes WHERE deprecated = 0 "
                 "                                 AND key NOT IN (SELECT key FROM temp) ",
            on_success="All deprecated entries are either singular initial or final and diff, with the "
                       "diff being the newest diff record in episodes table",
            on_failure="Found Entries that were not deprecated but not singular initial or final and "
                       "diff, with the diff being the newest diff record in episodes table",
            epilogue=["DROP TABLE IF EXISTS temp"]
        )

        self.logger.info("Done with Episodes Table Checks")

        return res

    # ==================================================================================================================
    # Checks for the Stream Table
    # ==================================================================================================================

    def check_stream_table(self):
        """
        Check the stream table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Streams Table")
        self.logger.info("=" * 120)

        res = False

        # Checking for entries without a last_seen field.
        res = res or self._perform_check(
            stmt="SELECT * FROM streams WHERE last_seen IS NULL",
            on_failure="Found records with empty last_seen field in streams table",
            on_success="No records with empty last_seen field found in streams table")

        res = res or self._perform_check(
            stmt="SELECT * FROM streams WHERE found IS NULL",
            on_failure="Found records with empty found field in streams table",
            on_success="No records with empty found field found in streams table")

        self.logger.info("Done with Streams Table Checks")
        return res

    # ==================================================================================================================
    # Checks for the Metadata_Episode_Assoz Table
    # ==================================================================================================================

    def check_metadata_episode_assoz_table(self):
        """
        Check the assoz table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Metadata_Episode_Assoz Table")
        self.logger.info("=" * 120)

        res = False

        # Find dangling records based on episode_key
        res = res or self._perform_check(
            stmt="SELECT key FROM metadata_episode_assoz "
                 "WHERE metadata_episode_assoz.episode_key NOT IN (SELECT key FROM episodes)",
            on_success="Found no dangling episode keys in metadata_episode_assoz",
            on_failure="Found dangling episode keys in metadata_episode_assoz")

        # Find dangling records based on metadata_key
        res = res or self._perform_check(
            stmt="SELECT key FROM metadata_episode_assoz "
                 "WHERE metadata_episode_assoz.metadata_key NOT IN (SELECT key FROM metadata)",
            on_success="Found no dangling metadata keys in metadata_episode_assoz",
            on_failure="Found dangling metadata keys in metadata_episode_assoz")

        # Check no links to final records
        res = res or self._perform_check(
            stmt="SELECT * FROM metadata_episode_assoz "
                 "WHERE metadata_key IN (SELECT key FROM metadata WHERE record_type = 2) "
                 "OR episode_key IN (SELECT key FROM episodes WHERE record_type = 2);",
            on_success="No links to final record in metadata_episode_assoz tables",
            on_failure="Found links to final record in metadata_episode_assoz tables")

        self.logger.info("Done with Metadata_Episode_Assoz Table Checks")
        return res

    # ==================================================================================================================
    # Checks for the Episode_Stream_Assoz Table
    # ==================================================================================================================

    def check_episode_stream_assoz_table(self):
        """
        Check the assoz table for dangling records
        """
        self.logger.info("=" * 120)
        self.logger.info("Performing Sanity Checks of the Episode_Streams_Assoz Table")
        self.logger.info("=" * 120)

        res = False

        # Find dangling records based on episode_key
        res = res or self._perform_check(
            stmt="SELECT key FROM episode_stream_assoz "
                 "WHERE episode_stream_assoz.episode_key NOT IN (SELECT key FROM episodes)",
            on_success="Found no dangling episode keys in episode_stream_assoz",
            on_failure="Found dangling episode keys in episode_stream_assoz")

        # Find dangling records based on metadata_key
        res = res or self._perform_check(
            stmt="SELECT key FROM episode_stream_assoz "
                 "WHERE episode_stream_assoz.stream_key NOT IN (SELECT key FROM streams)",
            on_success="Found no dangling stream keys in episode_stream_assoz",
            on_failure="Found dangling stream keys in episode_stream_assoz")

        # Check no links to final records
        res = res or self._perform_check(
            stmt="SELECT * FROM episode_stream_assoz "
                 "WHERE episode_key IN (SELECT key FROM episodes WHERE record_type = 2);",
            on_success="No links to final record in episode_stream_assoz tables",
            on_failure="Found links to final record in episode_stream_assoz tables")

        self.logger.info("Done with Episode_Streams_Assoz Table Checks")

        return res


if __name__ == "__main__":
    l = logging.getLogger("sanity_checker")
    l.setLevel(logging.DEBUG)
    h = logging.StreamHandler()
    h.setLevel(logging.DEBUG)
    l.addHandler(h)
    l.propagate = False
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"

    sc1 = SanityCheck(path)
    if sc1.check_all():
        print(f"WARNING: At least one sanity check didn't pass", file=sys.stderr)
    else:
        print(f"INFO: All Sanity Checks Passed", file=sys.stderr)
    # sc1.check_metadata_episode_assoz_table()
    # sc1.check_episode_stream_assoz_table()

    path_b64 = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    sc2 = SanityCheck(path_b64)
    sc2.check_all()
    # sc2.check_metadata_episode_assoz_table()
    # sc2.check_episode_stream_assoz_table()