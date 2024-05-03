import datetime
import os.path

from eth_loader.site_indexer import ConcurrentETHSiteIndexer
from eth_loader.metadata_loader import EpisodeLoader
from eth_loader.stream_loader import BetterStreamLoader, SpecLogin
from secrets import user_name, password, spec_login
from logs import setup_logging


workers = 10

def perform_index_of_sites(db: str):
    global workers
    print("Started")
    index_start = datetime.datetime.now()
    eid = ConcurrentETHSiteIndexer(db)
    eid.index_video_eth(threads=workers)
    eid.gen_parent()
    end = datetime.datetime.now()
    print(f"required {(end - index_start).total_seconds()}s")


def download_all_metadata(db, index_start: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    print(index_start)
    eid = EpisodeLoader(db, use_base64=b64)
    eid.download(index_start, workers)
    eid.deprecate(dt=index_start)
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds()}s")


def download_all_stream_data(db: str, index_start: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    bsl = BetterStreamLoader(db=db, user_name=user_name, password=password, spec_login=spec_login, use_base64=b64)
    bsl.initiator(workers=workers)
    bsl.deprecate(index_start)
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds()}s")


if __name__ == "__main__":
    debug_path = os.path.join(os.path.dirname(__file__), "logging_debug.yaml")
    production_path = os.path.join(os.path.dirname(__file__), "logging_production.yaml")
    setup_logging(default_path=debug_path)
    # setup_logging(default_path=production_path)
    global_start = datetime.datetime.now()
    path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites_b64.db"
    # path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites.db"
    perform_index_of_sites(path)
    download_all_metadata(path, global_start, b64=True)
    download_all_stream_data(path, global_start, b64=True)

    global_end = datetime.datetime.now()
    print(f"Overall Time for complete indexing: {(global_end - global_start).total_seconds():.02f}")
