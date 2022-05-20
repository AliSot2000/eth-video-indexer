import requests as rq
from lxml import etree
from lxml.etree import _Element
import multiprocessing as mp
from threading import Thread
from time import sleep
from queue import Empty


def is_video(root: _Element) -> bool:
    video = root.xpath("//vp-episode-page")

    if len(video) > 0:
        return True

    return False


class ETHIndexer:
    def __init__(self, file: str):
        self.prefixes = ["/campus", "/conferences", "/events", "/speakers", "/lectures"]
        self.file = file

    def index_video_eth(self):
        urls = []

        # load main site
        resp = rq.get("https://www.video.ethz.ch/", headers={"user-agent": "Mozilla Firefox"})

        # get the html
        html = resp.content.decode("utf-8")

        # prepare for xpath
        tree = etree.HTML(html)
        x = tree.xpath("//a")

        # find all <a> elements
        for a in x:
            a: _Element

            # Verify that href is a key
            if "href" in a.keys():
                uri = a.attrib["href"]

                if self.val_uri(uri):
                    self.sub_index(f"https://www.video.ethz.ch{uri}", uri)

        print("------------------------------------------------------------------------------------------------------------------------\nDONE\n------------------------------------------------------------------------------------------------------------------------")

    def sub_index(self, url: str, prefix: str):
        self.__sub_index(url, prefix)

    def __sub_index(self, url: str, prefix: str):
        # load main site
        resp = rq.get(url, headers={"user-agent": "Mozilla Firefox"})

        # get the html
        html = resp.content.decode("utf-8")

        # prepare for xpath
        tree = etree.HTML(html)
        # x = tree.xpath("//a")
        x = tree.xpath("//div[@class='newsListBox']/a")

        # dump the site to the list of video urls if it matches
        if is_video(tree):
            with open(self.file, "a") as f:
                f.write(url)
                f.write("\n")
            return

        # find all <a> elements
        for a in x:
            a: _Element

            # asure href is a key
            if "href" in a.keys():
                uri = a.attrib["href"]

                # verify it is on the same branch but not the same uri
                if (prefix.split(".")[0] in uri) and (prefix != uri):
                    self.sub_index(f"https://www.video.ethz.ch{uri}", uri)

        print(f"Done {url}")

    def val_uri(self, url: str) -> bool:
        # it is a valid site
        if ".html" not in url:
            return False

        # it is a child
        if len(url.split("/")) > 2:
            return False

        # it is a desired url
        for pref in self.prefixes:
            if pref in url:
                return True

        return False


class ConcurrentETHIndexer(ETHIndexer):
    def __init__(self, file: str, prefixes: list = None):
        """
        Initializer for concurrent indexing of entire viedeo.ethz.ch site.

        At the time permitted indexes are: campus, conferences, events, speakers, lectures

        :param file: output where the video-series urls are stored. (at the time 6460 urls)
        :param prefixes: provide custom prefixes, main_header [campus, lectures, ...]
        """
        super().__init__(file)
        self.to_download_queue = mp.Queue()
        self.video_url_queue = mp.Queue(maxsize=100)
        self.threads = []
        self.index_video_eth()

    def index_video_eth(self):
        """
        Starts the indexing of the site.
        :return:
        """
        # load main site
        resp = rq.get("https://www.video.ethz.ch/", headers={"user-agent": "Mozilla Firefox"})

        # get the html
        html = resp.content.decode("utf-8")

        # prepare for xpath
        tree = etree.HTML(html)
        x = tree.xpath("//a")

        # find all <a> elements
        for a in x:
            a: _Element

            # Verify that href is a key
            if "href" in a.keys():
                uri = a.attrib["href"]

                if self.val_uri(uri):
                    self.sub_index(f"https://www.video.ethz.ch{uri}", uri)

        self.spawn()
        self.dequeue()
        print("Cleanup")
        self.cleanup()

    def __sub_index(self, url: str, prefix: str):
        """
        Function that loads sub site and then proceeds to search it for either a video div or a list of sub sites.
        Sub sites are put into the to_download queue, video urls are put in the video_url queue

        :param url: full url of sub site to index like https://www.video.ethz.ch/speakers/d-infk/2015.html
        :param prefix: prefix of the site, only searching urls with identical prefix, like /speakers/d-infk
        :return:
        """
        # load main site
        resp = rq.get(url, headers={"user-agent": "Mozilla Firefox"})

        # get the html
        html = resp.content.decode("utf-8")

        # prepare for xpath
        tree = etree.HTML(html)
        # x = tree.xpath("//a")
        x = tree.xpath("//div[@class='newsListBox']/a")

        # dump the site to the list of video urls if it matches
        if is_video(tree):
            print(f"put {url}")
            self.video_url_queue.put(url)
            return

        # find all <a> elements
        for a in x:
            a: _Element

            # asure href is a key
            if "href" in a.keys():
                uri = a.attrib["href"]

                # verify it is on the same branch but not the same uri
                if (prefix.split(".")[0] in uri) and (prefix != uri):
                    self.sub_index(f"https://www.video.ethz.ch{uri}", uri)

        print(f"Done {url}")

    def cleanup(self):
        """
        Waits for all worker processes to terminate and then joins them.
        :return:
        """
        for worker in self.threads:
            worker: Thread
            worker.join()

    def spawn(self, threads: int = 100):
        """
        Spawns worker theads.

        :param threads: number of threads to spawn 1-10000
        :return:
        """
        if not 1 < threads < 10000:
            raise ValueError("Thread number outside supported range [1:10'000]")

        for i in range(threads):
            t = Thread(target=self.__indexer)
            t.start()
            self.threads.append(t)

        print("Workers Spawned")

    def __indexer(self):
        """
        Function executed by a worker thread. If the to_download queue is empty for 10s,
        the worker kills itself. For every target in the queue, it calls the __sub_index func.
        :return:
        """
        # after timeout of 10s the indexer subprocess kills itself
        counter = 0
        while counter < 10:
            try:
                # dequeue the next element
                target = self.to_download_queue.get(block=False)

                # perform sub index task
                self.__sub_index(target["url"], target["prefix"])

                # reset timeout
                counter = 0

            # sleep and timeout if queue is empty
            except Empty:
                counter += 1
                sleep(1)

    def sub_index(self, url: str, prefix: str):
        """
        Wrapper for __sub_index function.

        :param url: full url of sub site to index like https://www.video.ethz.ch/speakers/d-infk/2015.html
        :param prefix: prefix of the site, only searching urls with identical prefix, like /speakers/d-infk
        :return:
        """
        self.to_download_queue.put({"url": url, "prefix": prefix})

    def dequeue(self):
        """
        Function retrieves the urls from the video_url queue and writes them to the file specified in init.
        Function exits after 10s of an empty queue or when all workers are done.
        :return:
        """

        # Timeout to prevent endless loop if a subprocesses crash
        counter = 0
        while counter < 10 and self.workers_alive():
            if not self.video_url_queue.empty():
                with open(self.file, "a") as f:
                    while not self.video_url_queue.empty():
                        f.write(self.video_url_queue.get())
                        f.write("\n")
                counter = 0
            else:
                counter += 1
                sleep(1)

    def workers_alive(self):
        """
        Function to verify that at least one of the workers hasn't exited.
        :return:
        """
        for worker in self.threads:
            worker: Thread
            if worker.is_alive():
                return True

        return False