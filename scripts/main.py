import datetime
from eth_loader.site_indexer import ConcurrentETHSiteIndexer
from eth_loader.metadata_loader import EpisodeLoader
from eth_loader.stream_loader import BetterStreamLoader, SpecLogin
from secrets import user_name, password, spec_login

if __name__ == "__main__":
    global_start = datetime.datetime.now()
    path = "/home/alisot2000/Documents/01_ReposNCode/ETH-Lecture-Indexer/scripts/seq_sites.db"
    index_start = datetime.datetime.now()
    print("Started")
    eid = ConcurrentETHSiteIndexer(path)
    eid.index_video_eth(threads=10)
    eid.gen_parent()
    end = datetime.datetime.now()
    print(f"required {(end - index_start).total_seconds()}s")

    start = datetime.datetime.now()
    print("Started")
    eid = EpisodeLoader(path)
    eid.download(10)
    eid.deprecate(dt=index_start)
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds()}s")

    start = datetime.datetime.now()
    print("Started")
    eid = BetterStreamLoader(db=path, user_name=user_name, password=password, spec_login=spec_login)
    eid.initiator(workers=10)
    end = datetime.datetime.now()
    print(f"required {(end - start).total_seconds()}s")
    global_end = datetime.datetime.now()
    print(f"Overall Time for complete indexing: {(global_end - global_start).total_seconds()}")