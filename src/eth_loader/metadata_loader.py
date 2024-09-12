import datetime
import json
import logging
import multiprocessing as mp
import queue
import threading
import time

import jsondiff as jd
import requests as rq

from eth_loader.aux import from_b64, to_b64
from eth_loader.base_sql import BaseSQliteDB


def retrieve_metadata(website_url: str, identifier: str, headers: dict, parent_id: int = -1) -> dict:
    """
    Function to download a single metadata file for a given video_site. The website_url needs to be of type:

    /category/subcategory/year/season/lecture_id.html

    or:

    /category/subcategory/site.html

    The function **expects** to receive a valid lecture url. If an url is provided, that doesn't contain a video,
    it will try to download it anyway. The function shouldn't fail except if the site doesn't exist.

    :param parent_id: id of parent in sites table
    :param website_url: url to download eg. /category/subcategory/year/season/lecture_id.html
    :param identifier: str for thread to give information where the download was executed in case of an error.
    :param headers: dict to be passed to the request library. Download will fail if no user-agent is provided.
    """
    logger = logging.getLogger("metadata_loader")
    url = website_url.replace(".html", ".series-metadata.json").replace("\n", "")

    try:
        result = rq.get(url=url, headers=headers)
    except Exception as e:
        logger.exception(f"{identifier:.02} failed to download {url}", exc_info=e)
        return {"url": url, "parent_id": parent_id, "status": -1, "content": None}

    content = None
    # https://www.asdf.com/path?args

    if result.ok:
        content = result.content.decode("utf-8")
    else:
        # INFO, also dequeue worker will warn
        logger.error(f"{identifier:.02} error {result.status_code}, {url}")

    # only everything after .com or something
    path = url.split("/", 3)[3]

    # remove get arguments
    path = path.split("?")[0]
    logger.debug(f"{identifier} Done {url}")

    # conform json
    try:
        content = json.dumps(json.loads(content), sort_keys=True)
    except json.JSONDecodeError as e:
        logger.error(f"{identifier:.02} Failed to decode json from {url}", exc_info=e)
        # keep the same content as before
    except Exception as e:
        logger.error(f"{identifier:.02} Failed to decode json from {url}", exc_info=e)

    return {"url": url, "parent_id": parent_id, "status": result.status_code, "content": content}


def handler(worker_nr: int, command_queue: mp.Queue, result_queue: mp.Queue):
    """
    Function executed in a worker thread. The function tries to download the given url in the queue. If the queue is
    empty for 20s, it will kill itself.

    :param worker_nr: Itendifier for debugging
    :param command_queue: Queue containing dictionaries containing all relevant information for downloading
    :param result_queue: Queue to put the results in. Handled in main thread.
    :return:
    """
    logger = logging.getLogger("metadata_loader")
    logger.info(f"Starting worker {worker_nr}")
    local_logger = logging.getLogger("thread_handler")
    local_logger.info(f"Worker {worker_nr:02} started")

    ctr = 0
    while ctr < 60:
        try:
            arguments = command_queue.get(block=False)
            ctr = 0
        except queue.Empty:
            ctr += 1
            time.sleep(1)
            continue

        if arguments is None:
            ctr = 200
            break

        result = retrieve_metadata(arguments["url"], str(worker_nr),
                                   headers={"user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:100.0) "
                                                          "Gecko/20100101 Firefox/100.0"},
                                   parent_id=arguments["parent_id"])

        result_queue.put(result)
    logger.info(f"{worker_nr} Terminated")
    local_logger.info(f"Worker {worker_nr:02} exiting")
    return


class EpisodeLoader(BaseSQliteDB):
    def __init__(self, index_db: str, start_dt: datetime.datetime, use_base64: bool = False):
        """
        Initialise downloader function. Provide the function either with a file containing valid urls or a list of urls.

        Examples for urls:

        /category/subcategory/year/season/lecture_id.html

        /category/subcategory/site.html

        :param index_db: Database result of the indexer.
        """
        super().__init__(index_db)
        self.logger = logging.getLogger("metadata_loader")

        self.result_queue = mp.Queue()
        self.command_queue = mp.Queue()
        self.urls = []
        self.nod = 0

        self.workers = None

        self.genera_cookie = None
        self.ub64 = use_base64
        self.start_dt = start_dt

        self.check_results_table()

    def get_video_urls(self, dt: datetime.datetime):
        """
        Get all sites where a video is present from sites table.
        :return:
        """
        dts = dt.strftime("%Y-%m-%d %H:%M:%S")

        self.verify_args_table()
        # Need to check >= since we're now using the same datetime for consistency.
        self.debug_execute(f"SELECT key, url FROM sites WHERE IS_VIDEO=1 AND datetime(last_seen) >= datetime('{dts}')")
        self.urls = self.sq_cur.fetchall()

    def verify_args_table(self):
        """
        Verify that the sites table exists
        :return:
        """
        self.debug_execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sites'")

        if self.sq_cur.fetchone() is None:
            raise ValueError("didn't find the 'sites' table inside the given database.")

    def check_results_table(self):
        """
        Check if the results table exists and create it if necessary.
        :return:
        """
        self.debug_execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")

        if self.sq_cur.fetchone() is None:
            # Record Types are 0, initial, 1 differential, 2 current final
            self.debug_execute("CREATE TABLE metadata "
                                "(key INTEGER PRIMARY KEY AUTOINCREMENT, "
                                "parent INTEGER, "  # Reference to the entry in the the sites table
                                "URL TEXT , "
                                "json TEXT,"
                                "deprecated INTEGER DEFAULT 0 CHECK (metadata.deprecated IN (0, 1)),"
                                "found TEXT,"
                                "last_seen TEXT, "
                                "json_hash TEXT, "
                                "record_type INTEGER CHECK (metadata.record_type IN (0, 1, 2)))")
            self.debug_execute("CREATE INDEX metadata_key_index ON metadata (key)")
            self.debug_execute("CREATE INDEX metadata_url_parent_index ON metadata (URL, parent)")

        self.debug_execute("CREATE INDEX IF NOT EXISTS metadata_key_index ON metadata (key)")
        self.debug_execute("CREATE INDEX IF NOT EXISTS metadata_url_parent_index ON metadata (URL, parent)")

    def cleanup_workers(self):
        """
        Waits for all worker processes to terminate and then joins them.
        :return:
        """
        for worker in self.workers:
            worker: threading.Thread
            worker.join(10)

    def download(self, dt: datetime.datetime, workers: int = 100):
        """
        Initiate main Download of the urls provided in the init method.
        Calls the enqueue function and then the check_result function.

        Either choose multi_file, then the target_dir is also evaluated and the site structure is stored inside the dir
        or the entire metadata is stored inside the target file. **WARNING the target file WILL be overwritten**

        :param dt: datetime of last site indexing
        :param workers: number of worker threads to run concurrently
        :return:series
        """
        self.get_video_urls(dt)
        self.enqueue_th(workers)
        self.check_result()
        self.cleanup_workers()
        self.sq_con.commit()
        self.logger.info("DOWNLOAD METADATA DONE")

    def spawn(self, workers: int):
        """
        Spawns the worker threads using threading package.

        :param workers: number of workers to spawn.
        :return:
        """
        # generate arguments for the worker threads
        commands = [(i, self.command_queue, self.result_queue,) for i in range(workers)]
        threads = []

        # spawn threads
        for command in commands:
            t = threading.Thread(target=handler, args=command)
            t.start()
            threads.append(t)

        self.workers = threads

    def enqueue_th(self, workers):
        """
        Function to load the urls and put them inside a queue for the workers to download them. Also spawns the
        worker threads.

        :param workers: number of workers. At least 1 maybe at most 10'000
        :return:
        """

        self.spawn(workers)

        if self.urls is not None:
            self.nod = len(self.urls)
            for url in self.urls:
                self.command_queue.put({"url": url[1], "parent_id": url[0]})
        else:
            raise ValueError("Database apparently doesn't have any urls, get_urls retrieved None")

        for i in range(workers):
            self.command_queue.put(None)

    def check_result(self):
        """
        Function check the results in the results queue.

        Either choose multi_file, then the target_dir is also evaluated and the site structure is stored inside the dir
        or the entire metadata is stored inside the target file. **WARNING the target file WILL be overwritten**
        :return:
        """
        g_counter = 0
        e_counter = 0
        e_url = []

        ctr = 0
        res = {"url": "Empty", "content": "empty"}
        while ctr < 20:
            if not self.result_queue.empty():
                try:
                    res = self.result_queue.get()
                    parent_id = res["parent_id"]
                    url = res["url"]

                    if res["status"] == 200:
                        self.insert_update_db(parent_id=parent_id, url=url, json_arg=res["content"])
                    else:
                        self.logger.error(f"Failed to download {url} with status code {res['status']}")
                        e_url.append(url)
                        e_counter += 1
                    ctr = 0
                except Exception as e:
                    self.logger.exception("Exception dequeue from result queue}", e, res)
                    e_counter += 1
                g_counter += 1
            else:
                time.sleep(1)
                ctr += 1

        self.logger.info(f"Downloaded {g_counter} with {e_counter} errors.")
        self.logger.info(f"Urls with failures:")
        for url in e_url:
            self.logger.info(url)

    def insert_update_db(self, parent_id: int, url: str, json_arg: str):
        """
        Insert into db if new, update if exists and check deprecation status.

        :param parent_id: id of the parent site in the sites table
        :param url: url of the metadata
        :param json_arg: json stored at metadata url (is already properly encoded for the db)
        :return:
        """
        # Prepare arguments for function
        now = self.start_dt.strftime("%Y-%m-%d %H:%M:%S")
        key, json_db, deprecated, record_type = None, None, None, None
        json_hash = hash(json_arg)
        conv_json_arg = to_b64(json_arg) if self.ub64 else json_arg.replace("'", "''")

        # check existence of initial and final record:
        self.debug_execute(f"SELECT key, json, deprecated, record_type FROM metadata "
                           f"WHERE parent = {parent_id} AND URL = '{url}' AND record_type IN (2, 0) "
                           f"ORDER BY record_type DESC LIMIT 1")

        results = self.sq_cur.fetchall()

        # check the length is 1, so we can unpack
        if len(results) != 0:
            assert len(results) == 1, "Update of the sql statement violates the assert - only one result expected"
            key, temp_json, deprecated, record_type = results[0]

            json_db = from_b64(temp_json) if self.ub64 else temp_json

            # Check the json matches
            if json_db == json_arg:

                # Differing logging messages depending on if the entry is deprecated or not
                if deprecated == 1:
                    self.logger.info(f"Reactivating deprecated entry in metadata: {url}")
                else:
                    self.logger.debug(f"Found active entry in metadata: {url}")

                # Perform the update of the differential entry that belongs to the final entry.
                if record_type == 2:
                    self.debug_execute(f"SELECT key FROM metadata "
                                       f"WHERE record_type = 1 AND parent = {parent_id} AND URL = '{url}' "
                                       f"ORDER BY found DESC LIMIT 1")
                    row = self.sq_cur.fetchone()

                    # Row mustn't be None, since we have a final record
                    assert row is not None, f"Couldn't find differential entry for {url}."
                    diff_key = row[0]
                    self.debug_execute(f"UPDATE metadata SET last_seen = '{now}', deprecated = 0 WHERE key = {diff_key}")

                # Else -> record_type == 0. The final or initial entry needs to be updated anyway, no else block needed
                # else:
                #     pass

                # Update the latest entry (i.e. the final entry or the initial entry if the json
                # has been the same all the time)
                assert record_type in (0, 2), f"Record type is {record_type}, expected 0 or 2"
                self.debug_execute(f"UPDATE metadata SET last_seen = '{now}', deprecated = 0 WHERE key = {key}")
                return

            # Else -> json is different, need to add something to the database. Don't update but insert for later diff
            # else:
            #     pass

        # Key would have been populated by now -> the variables is declared in both branches of the if statement
        # => no record found at all, so insert a new initial record
        if key is None:
            self.logger.debug(f"Inserting new entry to metadata: {url}")
            # Found a new url, is initial so record type 0
            self.debug_execute(
                f"INSERT INTO metadata (parent, URL, json, found, last_seen, record_type, json_hash) VALUES "
                f"({parent_id}, '{url}', '{conv_json_arg}', '{now}', '{now}', 0, '{json_hash}')")
        else:
            self.logger.debug(f"Adding new state of existing entry to metadata: {url}")
            # Does exist, but json is different, needs to be a new diff entry, so record_type is left empty
            # for diff building
            self.debug_execute(                                              # INFO, record_type is left empty
                f"INSERT INTO metadata (parent, URL, json, found, last_seen, json_hash) VALUES "
                f"({parent_id}, '{url}', '{conv_json_arg}', '{now}', '{now}', '{json_hash}')")

    def deprecate(self, dt: datetime.datetime):
        """
        Go through all entries of table. Make sure the parent has a last_seen newer than dt. If not, set deprecated to
        true.

        :param dt: limit before which a site is deemed to be deprecated.
        :return:
        """
        dts = dt.strftime("%Y-%m-%d %H:%M:%S")
        self.debug_execute(f"SELECT COUNT(key) FROM metadata "
                           f"WHERE datetime(last_seen) < datetime('{dts}') AND deprecated = 0")
        count = self.sq_cur.fetchone()[0]

        self.debug_execute(f"UPDATE metadata SET deprecated = 1 "
                           f"WHERE datetime(last_seen) < datetime('{dts}') AND deprecated = 0")

        self.sq_con.commit()
        self.logger.info(f"Deprecated {count} entries")
