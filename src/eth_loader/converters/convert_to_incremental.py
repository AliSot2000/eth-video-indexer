import json
import multiprocessing as mp
import os
import queue
import time
from typing import List, Dict, Union

import jsondiff as jd

from eth_loader.aux import from_b64, to_b64, escape_sql
from eth_loader.base_sql import BaseSQliteDB


def diff_for_row(data: dict, hid: int, test: bool):
    """
    :param hid: handler identifier (used for printing)
    :param test: if the generated diff should be tested for idempotency
    :param data: json strings to work on.

    Data is the following:
    - tbl: str - Name of the Table we're working in
    - b64: bool - if the json strings are encoded with b64 or not.
    - url: str - url of the given entries
    - rows: List[Dict[str, str]] - the Dicts are the single rows with colum name as the key and the col value as the
            dict value, the lst contains these rows

    """
    assert set(data.keys()) == {"rows", "tbl", "b64", "url"}, "Malformatted input"

    rows: List[Dict[str, str]] = data["rows"]
    tbl: str = data["tbl"]
    b64: bool = data["b64"]
    url: str = data["url"]

    stmts = []

    # update the first entry, set record type to initial i.e. 0
    stmts.append(f"UPDATE {tbl} SET record_type = 0 WHERE key = {rows[0]['key']}")

    # duplicate the last entry for keeping the state of the last index (i.e. record of type final i.e. 2)
    stmts.append(f"INSERT INTO {tbl} (URL, json, last_seen, record_type) VALUES "
                       f"('{url}','{escape_sql(rows[-1]['json'])}', '{rows[-1]['last_seen']}', 2)")

    # Compute the deltas
    deltas = {}
    for i in range(1, len(rows)):
        deltas[rows[i]['key']] = jd.diff(rows[i - 1]['json'], rows[i]['json'], load=True, dump=True)

    if test:
        # check the result is equivalent (so for my sanity)
        acc = rows[0]['json']
        for i in range(1, len(rows)):
            acc = jd.patch(acc, deltas[rows[i]['key']], load=True, dump=True)
            eq = json.loads(acc) == json.loads(rows[i]['json'])

            if not eq:
                print(f"{hid:02}: Error converting {tbl}")

                # Print the rows
                for row in rows:
                    print(f"{hid:02}: {row}")

                # Print the deltas
                for key, value in deltas:
                    print(f"{hid:02}: Key: {key}, Value: {value}")

                exit(1)

    # update the deltas
    for key, delta in deltas.items():
        new_json = to_b64(delta) if b64 else escape_sql(delta)
        stmts.append(f"UPDATE {tbl} SET json = '{new_json}', record_type = 1 WHERE key = {key}")

    return stmts


def diff_handler(test: bool, hid: int, in_q: mp.Queue, out_q: mp.Queue):
    """
    Handler function that is executed in multiple processes to perform the diff of the json.
    """
    print(f"{hid:02}: Started")

    # timeout of 60s
    count = 0
    while count < 60:
        try:
            data: dict | None = in_q.get(block=False)
            count = 0
        except queue.Empty:
            count += 1
            time.sleep(1)
            continue

        # enqueue Nones to indicate we're done.
        if data is None:
            break

        # perform the diff for these rows.
        stmts = diff_for_row(hid=hid, test=test, data=data)
        out_q.put(stmts)

    print(f"{hid:02}: exiting...")


class ConvToIncremental(BaseSQliteDB):
    proc_count: int
    use_mp: bool = True

    workers: Union[List[mp.Process],  None] = None
    cmd_queue: mp.Queue
    result_queue: mp.Queue

    # Used to see if the functions are working correctly.
    __test = True

    def __init__(self, db_path: str, b64: bool = False):
        super().__init__(db_path)
        self.b64 = b64
        self.proc_count = os.cpu_count()
        self.cmd_queue = mp.Queue()
        self.result_queue = mp.Queue()

    def convert(self):
        """
        Convert the database to incremental
        """
        self.prepare_tables()
        self.convert_singles()
        self.convert_multiples_metadata()
        self.convert_multiples_episodes()
        self.sanity_check()

    def prepare_tables(self):
        """
        Add the necessary columns to the tables
        """
         # Record Types are 0, initial, 1 differential, 2 current final
        print("Adding column to metadata")
        self.debug_execute("ALTER TABLE metadata ADD COLUMN record_type CHECK (metadata.record_type IN (0, 1, 2))")

        print("Adding column to episodes")
        self.debug_execute("ALTER TABLE episodes ADD COLUMN record_type CHECK (episodes.record_type IN (0, 1, 2))")

    def convert_singles(self):
        """
        Update the record types of urls that only appear once
        """
        # get the count for info to logging
        self.debug_execute("SELECT COUNT(*) FROM (SELECT * FROM metadata GROUP BY URL HAVING COUNT(*) = 1)")
        cnt = self.sq_cur.fetchone()[0]
        print(f"Found {cnt} URLs that only appear once in metadata, updating type to initial")

        # Perform the update
        self.debug_execute("UPDATE metadata SET record_type = 1 WHERE URL IN "
                           "(SELECT URL FROM metadata GROUP BY URL HAVING COUNT(URL) = 1)")

        # Get the count for logging
        self.debug_execute("SELECT COUNT(*) FROM (SELECT * FROM episodes GROUP BY URL HAVING COUNT(*) = 1)")
        cnt = self.sq_cur.fetchone()[0]
        print(f"Found {cnt} URLs that only appear once in episodes, updating type to initial")

        # Perform the update
        self.debug_execute("UPDATE episodes SET record_type = 1 WHERE URL IN "
                           "(SELECT URL FROM episodes GROUP BY URL HAVING COUNT(URL) = 1)")

    def _start_workers(self):
        """
        Start the workers for multiprocessed diffing
        """
        if self.workers is not None:
            raise ValueError("workers aren't none. Only one process at a time.")

        self.workers = [mp.Process(target=diff_handler, args=(self.__test, i, self.cmd_queue, self.result_queue))
                        for i in range(self.proc_count)]

        for worker in self.workers:
            worker.start()

    def _is_one_alive(self):
        """
        Check if at least one of the workers is still alive
        """
        alive = False
        for i in range(len(self.workers)):
            worker = self.workers[i]
            alive = worker.is_alive() or alive
            if worker.is_alive():
                print(f"Worker: {i} is still alive")

        return alive

    def _send_stop(self):
        """
        Send stop signal to workers
        """
        for _ in range(self.proc_count):
            self.cmd_queue.put(None)

    def _stop_workers(self):
        """
        Stop the workers and reset the variables.

        All workers have exited once this function returns
        """
        for w in self.workers:
            if w.is_alive():
                print(f"Worker was still alive. Killing")
                w.kill()

            w.join()

        self.workers = None

    def _parallel_converter(self, tbl: str):
        """
        Convert the table in parallel.

        - get all urls
        - prefill queue with #elements = #processes
        - start the processes
        - swap result with new task
        - once all tasks are done, wait for all processes to terminate
        - empty queue
        - cleanup processes
        """
        while not self.result_queue.empty():
            print(f"Results Queue not empty: {self.result_queue.get()}")

        while not self.cmd_queue.empty():
            print(f"Command Queue not empty: {self.cmd_queue.get()}")

        self.debug_execute(f"SELECT URL FROM {tbl} GROUP BY URL HAVING COUNT(URL) > 1 "
                           f"ORDER BY COUNT(URL) DESC, LENGTH(json) DESC")
        urls_raw = self.sq_cur.fetchall()
        urls = [u[0] for u in urls_raw]
        print(f"Found {len(urls)} URLs that appear more than once in metadata")

        # Prefill the queue
        for i in range(self.proc_count):
            url = urls.pop(0)
            rows = self.get_rows(url=url, tbl=tbl)
            data = {
                "rows": rows,
                "tbl": tbl,
                "b64": self.b64,
                "url": url
            }
            self.cmd_queue.put(data)

        # start parallel processes
        self._start_workers()

        # still have urls to go
        while len(urls) > 0:
            # Handle the result
            res = self.result_queue.get(block=True)

            assert isinstance(res, list), f"wrong return type {type(res).__name__}"

            for stmt in res:
                assert isinstance(stmt, str), f"wrong statement type {type(stmt).__name__}"
                self.debug_execute(stmt)

            # Add the new task
            url = urls.pop(0)
            rows = self.get_rows(url=url, tbl=tbl)
            data = {
                "rows": rows,
                "tbl": tbl,
                "b64": self.b64,
                "url": url
            }
            self.cmd_queue.put(data)

        # done with all urls
        assert len(urls) == 0, "Expecting to be done with all urls and processes are consuming all tasks"
        self._send_stop()

        # Wait for workers to exit
        while self._is_one_alive() or not self.result_queue.empty():
            try:
                # Handle the result
                res = self.result_queue.get(block=False)

            except queue.Empty:
                time.sleep(1)
                continue

            assert isinstance(res, list), f"wrong return type {type(res).__name__}"

            for stmt in res:
                assert isinstance(stmt, str), f"wrong statement type {type(stmt).__name__}"
                self.debug_execute(stmt)

        self._stop_workers()


    def _sequential_convert(self, tbl: str):
        """
        Sequentially convert an sanity check all urls.
        """
        self.debug_execute(f"SELECT URL FROM {tbl} GROUP BY URL HAVING COUNT(URL) > 1 "
                           f"ORDER BY COUNT(URL) DESC, LENGTH(json) DESC")
        urls_raw = self.sq_cur.fetchall()
        urls = [u[0] for u in urls_raw]
        print(f"Found {len(urls)} URLs that appear more than once in metadata")

        for url in urls:
            rows = self.get_rows(url=url, tbl=tbl)

            # update the first entry, set record type to initial i.e. 0
            self.debug_execute(f"UPDATE {tbl} SET record_type = 0 WHERE key = {rows[0]['key']}")

            # duplicate the last entry for keeping the state of the last index (i.e. record of type final i.e. 2)
            self.debug_execute(f"INSERT INTO {tbl} (URL, json, last_seen, record_type) VALUES "
                               f"('{url}','{escape_sql(rows[-1]['json'])}', '{rows[-1]['last_seen']}', 2)")

            # Compute the deltas
            deltas = {}
            for i in range(1, len(rows)):
                deltas[rows[i]['key']] = jd.diff(rows[i - 1]['json'], rows[i]['json'], load=True, dump=True)

            # check the result is equivalent (so for my sanity)
            if self.__test:
                acc = rows[0]['json']
                for i in range(1, len(rows)):
                    acc = jd.patch(acc, deltas[rows[i]['key']], load=True, dump=True)
                    eq = json.loads(acc) == json.loads(rows[i]['json'])

                    if not eq:
                        print(f"Error converting {tbl}")
                        print(rows)
                        print(deltas)
                        exit(1)

            # update the deltas
            for key, delta in deltas.items():
                new_json = to_b64(delta) if self.b64 else escape_sql(delta)
                self.debug_execute(f"UPDATE {tbl} SET json = '{new_json}', record_type = 1 WHERE key = {key}")

    def get_rows(self, url: str, tbl: str) -> List[Dict[str, str]]:
        """
        Get the rows associated with a url,
        """
        self.debug_execute(
            f"SELECT key, json, last_seen FROM {tbl} WHERE URL = '{url}' ORDER BY DATETIME(found) ASC")
        raw = self.sq_cur.fetchall()

        print(f"Converting {tbl} for {url} with {len(raw)} entries", flush=True, end="\r")
        # print(f"Converting {tbl} for {url} with {len(raw)} entries")

        # parse the rows
        if not self.b64:
            return [{"key": r[0], "json": r[1], "last_seen": r[2]} for r in raw]
        else:
            return [{"key": r[0], "json": from_b64(r[1]), "last_seen": r[2]} for r in raw]

    def generic_table_converter(self, tbl: str):
        """
        Convert the data format of the table of all non-trivial entries.

        Assumptions about the table:
        - The table has a URL column
        - The table has a json column
        - The table has a last_seen column
        - The table has a record_type column

        :param tbl: table name
        """
        if self.use_mp:
            self._parallel_converter(tbl)
        else:
            self._sequential_convert(tbl)


    def convert_multiples_metadata(self):
        """
        Convert the data format of the metadata table of all non-trivial entries.
        """
        self.generic_table_converter("metadata")

    def convert_multiples_episodes(self):
        """
        Convert the data format of the episodes table of all non-trivial entries.
        """
        self.generic_table_converter("episodes")

    def _check_records(self, sql_stmt: str, on_success: str, on_fail: str):
        """
        SQL statement should return rows of failures. If no rows are returned, the on_success statement is printed,
        otherwise the on_fail statement is printed.
        """
        self.debug_execute(sql_stmt)
        res = self.sq_cur.fetchall()

        if len(res) == 0:
            print(on_success)
        else:
            print(on_fail)
            print(f"Printing {len(res)} results which failed.")
            for r in res:
                print(r)

    def sanity_check(self):

        # Check no initial
        self._check_records(sql_stmt="SELECT url "
                           "FROM metadata "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 0 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with no initial record found in metadata",
                            on_fail="Found urls with no initial record in metadata")

        self._check_records(sql_stmt="SELECT url "
                           "FROM episodes "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 0 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with no initial record found in episodes",
                            on_fail="Found urls with no initial record in episodes")

        # more than one initial
        self._check_records(sql_stmt="SELECT url "
                           "FROM metadata "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 0 THEN 1 ELSE 0 END) > 1;",
                            on_success="No urls with more than one initial record found in metadata",
                            on_fail="Found urls with more than one initial record in metadata")

        self._check_records(sql_stmt="SELECT url "
                           "FROM episodes "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 0 THEN 1 ELSE 0 END) > 1;",
                            on_success="No urls with more than one initial record found in episodes",
                            on_fail="Found urls with more than one initial record in episodes")

        # Diff but no final
        self._check_records(sql_stmt="SELECT url "
                           "FROM metadata "
                           "GROUP BY URL HAVING "
                                     "AND SUM(CASE WHEN record_type = 1 THEN 1 ELSE 0 END) > 0" # Has diff
                                     "AND SUM(CASE WHEN record_type = 2 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with diff and no final record found in metadata",
                            on_fail="Found urls with diff and no final record in metadata")

        self._check_records(sql_stmt="SELECT url "
                           "FROM episodes "
                           "GROUP BY URL HAVING "
                                     "AND SUM(CASE WHEN record_type = 1 THEN 1 ELSE 0 END) > 0" # Has diff
                                     "AND SUM(CASE WHEN record_type = 2 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with diff and no final record found in episodes",
                            on_fail="Found urls with diff and no final record in episodes")

        # more than one final
        self._check_records(sql_stmt="SELECT url "
                           "FROM metadata "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 2 THEN 1 ELSE 0 END) > 1;",
                            on_success="No urls with more than one final record found in metadata",
                            on_fail="Found urls with more than one final record in metadata")

        self._check_records(sql_stmt="SELECT url "
                           "FROM episodes "
                           "GROUP BY URL HAVING SUM(CASE WHEN record_type = 2 THEN 1 ELSE 0 END) > 1;",
                            on_success="No urls with more than one final record found in episodes",
                            on_fail="Found urls with more than one final record in episodes")

        # no diff but more than two values per url
        self._check_records(sql_stmt="SELECT url "
                           "FROM metadata "
                           "GROUP BY URL HAVING COUNT(*) > 1 AND SUM(CASE WHEN record_type = 1 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with no diff but more than one records found in metadata",
                            on_fail="Found urls with no diff but more than one records in metadata")

        self._check_records(sql_stmt="SELECT url "
                           "FROM episodes "
                           "GROUP BY URL HAVING COUNT(*) > 1 AND SUM(CASE WHEN record_type = 1 THEN 1 ELSE 0 END) = 0;",
                            on_success="No urls with no diff but more than one records found in episodes",
                            on_fail="Found urls with no diff but more than one records in episodes")

        # Check no links to final records
        self._check_records(sql_stmt="SELECT * FROM metadata_episode_assoz "
                                     "WHERE metadata_key IN (SELECT key FROM metadata WHERE record_type = 2) "
                                     "OR episode_key IN (SELECT key FROM episodes WHERE record_type = 2);",
                            on_success="No links to final record in metadata_episode_assoz tables",
                            on_fail="Found links to final record in metadata_episode_assoz tables")

        self._check_records(sql_stmt="SELECT * FROM episode_stream_assoz "
                                     "WHERE episode_key IN (SELECT key FROM episodes WHERE record_type = 2);",
                            on_success="No links to final record in episode_stream_assoz tables",
                            on_fail="Found links to final record in episode_stream_assoz tables")

        # check final records aren't found
        self._check_records(sql_stmt="SELECT key FROM metadata WHERE record_type = 2 AND found IS NOT NULL;",
                            on_success="All final records have no found date in metadata table",
                            on_fail="Found final records with a found date in metadata table")

        self._check_records(sql_stmt="SELECT key FROM episodes WHERE record_type = 2 AND found IS NOT NULL;",
                            on_success="All final records have no found date in episodes table",
                            on_fail="Found final records with a found date in episodes table")

        # if there's exactly one entry for a given url, the type is initial not final
        self._check_records(sql_stmt="SELECT URL FROM metadata GROUP BY URL HAVING COUNT(*) = 1 WHERE record_type != 0;",
                            on_success="All single URLS have an initial record in metadata table",
                            on_fail="Found single URLS have no initial record in metadata table")

        self._check_records(sql_stmt="SELECT URL FROM episodes GROUP BY URL HAVING COUNT(*) = 1 WHERE record_type != 0;",
                            on_success="All single URLS have an initial record in episodes table",
                            on_fail="Found single URLS have no initial record in episodes table")

        # TODO sanity check
        #    Checks:
        #     - if there's a diff, the diff and the incremental must be deprecated
        pass


if __name__ == "__main__":
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    c = ConvToIncremental(db_path=path, b64=True)
    c.convert()
    c.sanity_check()
