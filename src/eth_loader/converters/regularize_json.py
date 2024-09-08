import os

import queue
from eth_loader.base_sql import BaseSQliteDB
import json
from eth_loader.aux import from_b64
import multiprocessing as mp
import time

from typing import Union, List
from scripts.convert_b64 import to_b64


def convert_json(data: dict, b64: bool = False) -> dict:
    """
    Convert the json data to a dict
    """
    assert set(data.keys()) == {"key", "json"}, "Malformatted input"
    json_data = data["json"]
    key = data["key"]

    try:
        if b64:
            base_data = from_b64(json_data)
            formatted_data = json.dumps(json.loads(base_data), sort_keys=True)
            new_data = to_b64(formatted_data)
        else:
            new_data = json.dumps(json.loads(json_data), sort_keys=True)
    except json.JSONDecodeError as e:
        print(f"Error decoding json for key {key}: {e}")
        new_data = json_data

    if new_data != json_data:
        return {"key": key, "json": new_data}
    else:
        return {"key": key, "json": None}

def regularize_handler(hid: int, in_q: mp.Queue, out_q: mp.Queue, b64: bool = False):
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
        stmts = convert_json(data=data, b64=b64)
        out_q.put(stmts)

    print(f"{hid:02}: exiting...")

class RegularizeJSON(BaseSQliteDB):
    """
    Converter adds a has of the json data to the metadata and episodes table.
    Point being - I can detect possibly identical json based on hash
    """
    workers: Union[List[mp.Process],  None] = None
    cmd_queue: mp.Queue
    res_queue: mp.Queue
    proc_count: int

    def __init__(self, db_path: str, b64: bool = False):
        super().__init__(db_path)
        self.b64 = b64
        self.cmd_queue = mp.Queue()
        self.res_queue = mp.Queue()
        self.proc_count = os.cpu_count()
        if self.proc_count > 1000:
            print("Too many CPUs, limiting to 512")
            self.proc_count = 512


    def convert(self):
        """
        Convert both the metadata and the episodes table to have regularized json
        """
        self._generic_converter("metadata")
        self._generic_converter("episodes")

    def _generic_converter(self, tbl: str):
        """
        Generic converter function for table
        """
        self.debug_execute(f"SELECT key, json FROM {tbl} ORDER BY key ASC LIMIT 1000")
        raw_rows = self.sq_cur.fetchall()
        rows = [{"key": row[0], "json": row[1]} for row in raw_rows]
        index = rows[-1]["key"]
        done = False


        print(f"TODO: {tbl} has {len(rows)} rows")

        # Prepare queue
        for _ in range(self.proc_count):
            self.cmd_queue.put(rows.pop(0))

        # start the workers
        self._start_workers()

        # wait for all workers to finish
        while not done:
            # Handle the result
            res = self.res_queue.get(block=True)

            assert isinstance(res, dict), f"wrong return type {type(res).__name__}"
            assert set(res.keys()) == {"key", "json"}, "Malformatted input"

            if res["json"] is not None:
                new_json = res["json"].replace("'", "''")
                self.debug_execute(f"UPDATE {tbl} SET json = '{new_json}' WHERE key = {res['key']}")
                print(f"Updated key {res['key']:06} in {tbl}")
            else:
                print(f"No change for key {res['key']:06} in {tbl}")

            # Add the new task
            self.cmd_queue.put(rows.pop(0))

            # Update teh rows if empty
            if len(rows) == 0:
                self.debug_execute(f"SELECT key, json FROM {tbl} WHERE key > {index} ORDER BY key ASC LIMIT 1000")
                raw_rows = self.sq_cur.fetchall()
                rows = [{"key": row[0], "json": row[1]} for row in raw_rows]
                # We have reached the end, we need to stop
                if len(rows) == 0:
                    done = True
                    break
                index = rows[-1]["key"]
                done = False

        # Send stop signal
        self._send_stop()

        count = 0
        # Wait for all workers to finish
        while self._is_one_alive() or not self.res_queue.empty():
            try:
                res = self.res_queue.get(block=False)
                count = 0
            except queue.Empty:
                count += 1
                time.sleep(1)
                if count > 60:
                    break
                continue

            assert isinstance(res, dict), f"wrong return type {type(res).__name__}"
            assert set(res.keys()) == {"key", "json"}, "Malformatted input"

            if res["json"] is not None:
                new_json = res["json"].replace("'", "''")
                self.debug_execute(f"UPDATE {tbl} SET json = '{new_json}' WHERE key = {res['key']}")
                print(f"Updated key {res['key']:06} in {tbl}")
            else:
                print(f"No change for key {res['key']:06} in {tbl}")

        # Stop the workers
        self._stop_workers()

    # ==================================================================================================================
    # MP Functions
    # ==================================================================================================================

    def _start_workers(self):
        """
        Start the workers multiprocessing regularize_handler
        """
        if self.workers is not None:
            raise ValueError("workers aren't none. Only one process at a time.")

        self.workers = [mp.Process(target=regularize_handler, args=(i, self.cmd_queue, self.res_queue))
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


if __name__ == "__main__":
    path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"

    c = RegularizeJSON(db_path=path, b64=False)
    c.convert()
    c.sq_con.commit()
    c.cleanup()