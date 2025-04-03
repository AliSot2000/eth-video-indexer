import datetime
import os.path
import sys
import shutil

from eth_loader.diff_builder import IncrementBuilder
from eth_loader.metadata_loader import MetadataLoader
from eth_loader.sanity_check import SanityCheck
from eth_loader.site_indexer import ETHSiteIndexer
from eth_loader.stream_loader import BetterStreamLoader
from logs import setup_logging
from scripts.secrets import user_name, password, spec_login

workers = 10

def build_metadata_increment(db: str, b64: bool, start_dt: datetime.datetime):
    start = datetime.datetime.now()
    icb = IncrementBuilder(db_path=db, b64=b64, start_dt=start_dt)
    icb.build_increment_metadata()
    icb.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")


def build_episode_increment(db: str, b64: bool, start_dt: datetime.datetime):
    start = datetime.datetime.now()
    icb = IncrementBuilder(db_path=db, b64=b64, start_dt=start_dt)
    icb.build_increment_episodes()
    icb.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")

def perform_index_of_sites(db: str, dt: datetime.datetime):
    global workers
    print("Started")
    index_start = datetime.datetime.now()
    eid = ETHSiteIndexer(db_file=db, start_dt=dt)
    eid.index_video_eth(threads=workers)
    eid.gen_parent()
    eid.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - index_start).total_seconds():.02f}s")


def download_all_metadata(db, index_start: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    print(index_start)
    eid = MetadataLoader(db, use_base64=b64, start_dt=index_start)
    eid.download(index_start, workers)
    eid.deprecate(dt=index_start)
    eid.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")


def perform_deprecate_metadata(db: str, dt: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    eid = MetadataLoader(db, use_base64=b64, start_dt=dt)
    eid.deprecate(dt=dt)
    eid.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")


def perform_deprecate_episodes(db: str, dt: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    bsl = BetterStreamLoader(db=db,
                             user_name=user_name,
                             password=password,
                             spec_login=spec_login,
                             use_base64=b64,
                             start_dt=dt)
    bsl.deprecate(dt)
    bsl.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")

def download_all_stream_data(db: str, index_start: datetime.datetime, b64: bool = False):
    global workers
    start = datetime.datetime.now()
    print("Started")
    bsl = BetterStreamLoader(db=db,
                             user_name=user_name,
                             password=password,
                             spec_login=spec_login,
                             use_base64=b64,
                             start_dt=index_start)
    bsl.initiator(workers=workers)
    bsl.cleanup()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")


def sanity_check(db: str):
    sc = SanityCheck(db)
    start = datetime.datetime.now()
    sc.check_all()
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds():.02f}s")

def full_run(db_path: str, index_start: datetime.datetime, b64: bool):
    perform_index_of_sites(db_path, index_start)

    build_metadata_increment(db_path, b64=b64, start_dt=index_start)
    download_all_metadata(db_path, index_start, b64=b64)
    build_metadata_increment(db_path, b64=b64, start_dt=index_start)
    perform_deprecate_metadata(db_path, index_start, b64=b64)

    build_episode_increment(db_path, b64=b64, start_dt=index_start)
    download_all_stream_data(db_path, index_start, b64=b64)
    build_episode_increment(db_path, b64=b64, start_dt=index_start)
    perform_deprecate_episodes(db_path, index_start, b64=b64)

    if sanity_check(db_path):
        print(f"At least one Sanity Check Failed", file=sys.stderr)
    else:
        print(f"All Sanity Checks passed", file=sys.stderr)


if __name__ == "__main__":
    debug = False
    if debug:
        debug_path = os.path.join(os.path.dirname(__file__), "logging_debug.yaml")
        setup_logging(default_path=debug_path)
        print("DEBUGGING")
    else:
        production_path = os.path.join(os.path.dirname(__file__), "logging_production.yaml")
        setup_logging(default_path=production_path)
        print("PRODUCTION")

    global_start = datetime.datetime.now()
    # global_start = datetime.datetime(2024, 9, 23, 0, 0, 0)

    is_b64 = False
    both = True
    make_backup: bool = True

    if is_b64 or both:
        backup_path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db.bak"
        path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_b64.db"

        if make_backup:
            print("Making Backup of Database...")
            shutil.copy2(path, backup_path)
            print("Backup Done")

        full_run(path, global_start, b64=True)

    if not is_b64 or both:
        backup_path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db.bak"
        path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"

        if make_backup:
            print("Making Backup of Database...")
            shutil.copy2(path, backup_path)
            print("Backup Done")

        full_run(path, global_start, b64=False)

    global_end = datetime.datetime.now()
    print(f"Overall Time for complete indexing: {(global_end - global_start).total_seconds():.02f}")
