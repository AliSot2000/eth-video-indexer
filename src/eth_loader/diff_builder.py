import json
import logging
import multiprocessing as mp
import os
from datetime import datetime
from typing import List, Tuple
import queue

import time
import jsondiff as jd

import eth_loader.aux as aux
from eth_loader.base_sql import BaseSQliteDB


def build_diff(_b64: bool, target: dict, candidate: dict, tbl: str):
    # Asserts about the table
    assert tbl in ["episodes", "metadata"], f"Table {tbl} not recognized"
    assert set(target.keys()) == {'json', 'parent', 'url', 'found', 'json_hash', 'key'}

    # Asserts about the target
    if tbl == "episodes":
        assert target['parent'] is None, "Parent must be None for episodes"
    else:
        assert target['parent'] is not None, "Parent must not be None for metadata"


    assert set(candidate.keys()) == {'json_raw', 'record_type', 'key'}

    t = target
    c = candidate
    res = []

    # Conglomerate the json
    tgt_json = aux.from_b64(t['json']) if _b64 else t['json']
    c_json = aux.from_b64(c['json_raw']) if _b64 else c['json_raw']

    # Compute the diff
    json_diff = jd.diff(c_json, tgt_json, load=True, dump=True)

    # Regularize Json
    reg_json_diff = json.dumps(json.loads(json_diff), sort_keys=True)

    # Back convert to matching format
    diff_json_out = aux.to_b64(reg_json_diff) if _b64 else reg_json_diff.replace("'", "''")
    tgt_json_out = aux.to_b64(tgt_json) if _b64 else tgt_json.replace("'", "''")

    # Case 1: record_type 0, compute diff, and add a new final record, store diff in new incremental (now null)
    if c['record_type'] == 0:
        # INFO: Don't add an entry in the found column as this needs to be empty per definition of the
        #   final record.
        if tbl == "episodes":
            res.append(f"INSERT INTO episodes "
                               f"(URL, json, last_seen, record_type, json_hash) "
                               f"VALUES ('{t['url']}', '{tgt_json_out}', '{t['found']}', 2, {t['json_hash']})")
        elif tbl == "metadata":
            res.append(f"INSERT INTO metadata "
                               f"(parent, URL, json, last_seen, record_type, json_hash) "
                               f"VALUES ({t['parent']}, '{t['url']}', '{tgt_json_out}',"
                               f" '{t['found']}', 2, {t['json_hash']})")
        else:
            raise ValueError(f"Table {tbl} not recognized")

    # Case 2: compute diff between final and target, store diff in new incremental, update null to incremental
    # store new json in final, update final
    else:
        assert c['record_type'] == 2, f"Record type is {c['record_type']}, expected 2"

        # Update the final record with the new json
        res.append(f"UPDATE {tbl} SET json = '{tgt_json_out}', last_seen = '{t['found']}', "
                           f"json_hash = '{t['json_hash']}' WHERE key = {c['key']}")

    # Update the null record to incremental and store the diff in place of the full json
    res.append(f"UPDATE {tbl} "
                       f"SET json ='{diff_json_out}', record_type = 1 WHERE key = {t['key']}")

    return res

def diff_handler(worker_id: int, in_q: mp.Queue, out_q: mp.Queue, tbl: str, b64: bool):
        """
        Handler function that is executed in multiple processes to perform the diff of the json.
        """
        logger = logging.getLogger("increment_builder")
        local_logger = logging.getLogger("thread_handler")
        local_logger.info(f"{worker_id:02}: Started")

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
            try:
                stmts = build_diff(_b64=b64, target=data['target'], candidate=data['candidate'], tbl=tbl)
            except Exception as e:
                logger.error(f"{worker_id:02}: Error: {e}", exc_info=e)
                logger.debug(f"{worker_id:02}: Data: {data}")
                out_q.put([])
                continue

            out_q.put(stmts)

        local_logger.info(f"{worker_id:02}: exiting...")


class IncrementBuilder(BaseSQliteDB):
    """
    Given a database which has newly added objects with a NULL record_type, this class will build the diff,
    update the final and set the new diff record.
    """
    workers: List[mp.Process] | None = None
    task_count: int = -1
    cmd_queue = mp.Queue()
    result_queue = mp.Queue()

    timeout: int
    __parallel: bool = True

    def __init__(self, db_path: str, b64: bool, start_dt: datetime, workers: int = None, timeout: int = 300):
        """
        Initialize the class with the database path and the base64

        :param db_path: Path to the database
        :param b64: If the json is base64 encoded
        :param workers: Number of workers to use
        :param timeout: Timeout for ending draining pipeline
        :param start_dt: Start date of the database
        """
        super().__init__(db_path=db_path)
        self.ub64 = b64
        self.logger = logging.getLogger("increment_builder")
        self.start_dt = start_dt

        # Define number of workers
        if workers:
            if workers < 1:
                raise ValueError("Workers must be greater than 0")
            self.task_count = workers
        else:
            self.task_count = os.cpu_count()

        self.timeout = timeout

    def build_increment_metadata(self):
        """
        Build the increment for the metadata table
        """
        if self.__parallel:
            self._generic_parallel_diff_builder("metadata")
        else:
            self._sequential_metadata_diff_builder()

    def build_increment_episodes(self):
        """
        Build the increment for the episodes table
        """
        if self.__parallel:
            self._generic_parallel_diff_builder("episodes")
        else:
            self._sequential_episode_diff_builder()

    # ==================================================================================================================
    # Private functions to handle the busines of the class
    # ==================================================================================================================

    def _start_workers(self, tbl: str):
        """
        Start the workers for multiprocessed diffing
        """
        if self.workers is not None:
            raise ValueError("workers aren't none. Only one process at a time.")

        self.workers = [mp.Process(target=diff_handler, args=(i, self.cmd_queue, self.result_queue, tbl, self.ub64))
                        for i in range(self.task_count)]

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
                self.logger.debug(f"Worker: {i} is still alive")

        return alive

    def _send_stop(self):
        """
        Send stop signal to workers
        """
        for _ in range(self.task_count):
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

    def _generic_parallel_diff_builder(self, tbl: str):
        """
        Perform the parallel diff building
        """
        # Check preconditions for the function
        if not self.result_queue.empty() or not self.cmd_queue.empty():
            assert False, "Queue not empty, should be empty when this function is called"

        # Get the rows
        self.debug_execute(f"SELECT key FROM {tbl} WHERE record_type IS NULL ORDER BY LENGTH(json) DESC")
        keys = [r[0] for r in self.sq_cur.fetchall()]

        # Update the number of processes
        if len(keys) < self.task_count:
            self.task_count = len(keys)

        self.logger.info(f"Building differential records for {len(keys)} entries in {tbl}")

        # Prefill queue
        for x in range(self.task_count):
            self.cmd_queue.put(self._get_args(keys.pop(0), tbl))

        # Start the workers with filled queue
        self._start_workers(tbl=tbl)

        count = 0
        while len(keys) > 0 and count < self.timeout:
            try:
                res = self.result_queue.get(block=False)
                count = 0
            except queue.Empty:
                time.sleep(1)
                count += 1
                continue

            assert type(res) == list, f"Expected list, got {type(res)}"

            # Add a new argument
            self.cmd_queue.put(self._get_args(keys.pop(0), tbl))

            # Execute Statements for results
            for stmt in res:
                assert type(stmt) == str, f"Expected str, got {type(stmt)}"
                self.debug_execute(stmt)

        # Stop the workers
        self._send_stop()

        counter = 0

        # Wait for the workers to finish, and queue to empty
        while (self._is_one_alive() or not self.result_queue.empty()) and counter < self.timeout:
            try:
                res = self.result_queue.get(block=False)
                counter = 0
            except queue.Empty:
                time.sleep(1)
                counter += 1
                continue

            assert type(res) == list, f"Expected list, got {type(res)}"

            # Execute Statements for results
            for stmt in res:
                assert type(stmt) == str, f"Expected str, got {type(stmt)}"
                self.debug_execute(stmt)

        self._stop_workers()

    def _get_args(self, key: int, tbl: str):
        """
        Get the arguments for the concurrent program
        """
        # Get the information about the target
        if tbl == "episodes":
            self.logger.debug(f"Building diff for key: {key} in table {tbl}")

            self.debug_execute(f"SELECT json, URL, found, json_hash FROM episodes WHERE key = {key}")
            raw = self.sq_cur.fetchall()
            assert len(raw) == 1, f"Expected one row for key query, got {len(raw)} for URL"

            target =  {"json": raw[0][0],
                       "url": raw[0][1],
                       "found": raw[0][2],
                       "json_hash": raw[0][3],
                       "key": key,
                       "parent": None}

            # Get the next candidate for the differential record
            self.debug_execute(f"SELECT key, json, record_type "
                               f"FROM episodes WHERE URL = '{target['url']}' AND record_type IN (0, 1, 2) "
                               f"ORDER BY record_type DESC LIMIT 1")

            raw = self.sq_cur.fetchall()
            assert len(raw) == 1, f"Expected one row, got {len(raw)}"


        elif tbl == "metadata":
            self.debug_execute(f"SELECT json, parent, URL, found, json_hash FROM metadata WHERE key = {key}")

            raw = self.sq_cur.fetchall()
            assert len(raw) == 1, f"Expected one row for key query, got {len(raw)}"
            target = {
                "json": raw[0][0],
                "parent": raw[0][1],
                "url": raw[0][2],
                "found": raw[0][3],
                "json_hash": raw[0][4],
                "key": key
            }

            # Get the information about the candidate
            self.debug_execute(f"SELECT key, json, record_type "
                               f"FROM metadata WHERE URL = '{target['url']}' AND parent = {target['parent']} "
                               f"AND record_type IN (0, 1, 2) "
                               f"ORDER BY record_type DESC LIMIT 1")
            raw = self.sq_cur.fetchall()
            assert len(raw) == 1, f"Expected one row, got {len(raw)}, for URL, parent"

        else:
            raise ValueError(f"Table {tbl} not recognized")

        # Fetch the candidate information and return
        candidate = {"key": raw[0][0], "json_raw": raw[0][1], "record_type": raw[0][2]}

        # Printing information about mismatching start_dt
        if target["found"] != self.start_dt.strftime("%Y-%m-%d %H:%M:%S"):
            self.logger.warning(f"Entry with mismatching found date. Database not saved properly?, "
                              f"found {target['found']}")

        return {"target": target, "candidate": candidate}

    # ==================================================================================================================
    # Sequential Version for Sanity check
    # ==================================================================================================================

    def _sequential_episode_diff_builder(self):
        """
        For newly inserted episodes, build the differential records.
        """
        self.debug_execute("SELECT key FROM episodes WHERE record_type IS NULL")
        keys = [res[0] for res in self.sq_cur.fetchall()]

        self.logger.info(f"Building differential records for {len(keys)} entries in episodes")

        for key in keys:
            self.logger.debug(f"Processing new Record: {key:07}")

            # Get the rows:
            args = self._get_args(key, "episodes")
            t = args["target"]

            # check the found matches
            if t["found"] != self.start_dt.strftime("%Y-%m-%d %H:%M:%S"):
                self.logger.error(f"Entry with mismatching found date. Database not saved properly?, "
                                  f"found {t['found']}")

            c = args["candidate"]
            assert c['record_type'] != 1, "Found a differential record (1), fix SQL statement, 0 and 2 accepted"

            # Conglomerate the json
            tgt_json_out, diff_json_out = self.perform_diff(target_json=t['json'], candidate_json=c['json_raw'])

            # Case 1: record_type 0, compute diff, and add a new final record, store diff in new incremental (now null)
            if c['record_type'] == 0:
                # INFO: Don't add an entry in the found column as this needs to be empty per definition of the
                #   final record.
                self.debug_execute(f"INSERT INTO episodes "
                                   f"(URL, json, last_seen, record_type, json_hash) "
                                   f"VALUES ('{t['url']}', '{tgt_json_out}', '{t['found']}', 2, {t['json_hash']})")

            # Case 2: compute diff between final and target, store diff in new incremental, update null to incremental
            # store new json in final, update final
            else:
                assert c['record_type'] == 2, f"Record type is {c['record_type']}, expected 2"

                # Update the final record with the new json
                self.debug_execute(f"UPDATE episodes SET json = '{tgt_json_out}', last_seen = '{t['found']}', "
                                   f"json_hash = '{t['json_hash']}' WHERE key = {c['key']}")

            # Update the null record to incremental and store the diff in place of the full json
            self.debug_execute(f"UPDATE episodes "
                               f"SET json ='{diff_json_out}', record_type = 1 WHERE key = {t['key']}")

    def _sequential_metadata_diff_builder(self):
        """
        For newly inserted metadata, build the differential records.
        """
        self.debug_execute("SELECT key FROM metadata WHERE record_type IS NULL")
        keys = [res[0] for res in self.sq_cur.fetchall()]

        self.logger.info(f"Found {len(keys)} new diff entries to process for metadata")

        for key in keys:
            self.logger.debug(f"Processing new Record: {key:07}")

            args = self._get_args(key, "metadata")
            t = args["target"]

            c = args["candidate"]
            assert c['record_type'] != 1, "Found a differential record (1), fix SQL statement, 0 and 2 accepted"

            # Conglomerate the json
            tgt_json_out, diff_json_out = self.perform_diff(target_json=t['json'], candidate_json=c['json_raw'])

            # Case 1: record_type 0, compute diff, and add a new final record
            if c['record_type'] == 0:
                # INFO: Don't add an entry in the found column as this needs to be empty per definition of the
                #   final record.
                self.debug_execute(f"INSERT INTO metadata "
                                   f"(parent, URL, json, last_seen, record_type, json_hash) "
                                   f"VALUES ({t['parent']}, '{t['url']}', '{tgt_json_out}',"
                                   f" '{t['found']}', 2, {t['json_hash']})")

            # Case 2: compute diff between final and target, store diff in incremental,
            # store new json in final, update final
            else:
                assert c['record_type'] == 2, f"Record type is {c['record_type']}, expected 2"


                # Update the final record with the new json
                self.debug_execute(f"UPDATE metadata SET json = '{tgt_json_out}', last_seen = '{t['found']}', "
                                   f"json_hash = '{t['json_hash']}' WHERE key = {c['key']}")

            # Update the null record to incremental and store the diff in place of the full json
            self.debug_execute(f"UPDATE metadata "
                               f"SET json = '{diff_json_out}', record_type = 1 WHERE key = {t['key']}")



    def perform_diff(self, target_json: str, candidate_json: str) -> Tuple[str, str]:
        """
        Perform the diff between the target json string and the candidate json string

        :param target_json: The target json string
        :param candidate_json: The candidate json string

        return: Tuple of the target json string and the diff json string
        """
        tgt_json = aux.from_b64(target_json) if self.ub64 else target_json
        c_json = aux.from_b64(candidate_json) if self.ub64 else candidate_json

        # Compute the diff
        json_diff = jd.diff(c_json, tgt_json, load=True, dump=True)

        # Regularize Json
        reg_json_diff = json.dumps(json.loads(json_diff), sort_keys=True)

        # Back convert to matching format
        diff_json_out = aux.to_b64(reg_json_diff) if self.ub64 else reg_json_diff.replace("'", "''")
        tgt_json_out = aux.to_b64(tgt_json) if self.ub64 else tgt_json.replace("'", "''")
        return tgt_json_out, diff_json_out


if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"
    b64 = True
    db = IncrementBuilder(db_path=path, b64=b64)
    db.build_increment_metadata()
    db.build_increment_episodes()
    # db.cleanup()
