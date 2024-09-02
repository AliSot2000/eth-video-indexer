import jsondiff as jd
from eth_loader.base_sql import BaseSQliteDB
import json
import difflib

from scratch.diff_base import b_reconstructed


class Test(BaseSQliteDB):
    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.order = []
        self.snapshots = {}
        self.deltas = {}

    def get_snapshot(self):
        self.debug_execute(f"SELECT key, json, found FROM metadata WHERE URL = 'https://www.video.ethz.ch/campus/bibliothek/colloquium.series-metadata.json' ORDER BY found ASC ;")
        res = self.sq_cur.fetchall()

        for r in res:
            # print(r)
            self.order.append(r[0])
            self.snapshots[r[0]] = r[1]

    def get_deltas(self):
        for i in range(1, len(self.order)):
            self.deltas[self.order[i]] = jd.diff(self.snapshots[self.order[i-1]], self.snapshots[self.order[i]], load=True, dump=True)

    def test_deltas(self):
        acc = json.dumps(json.loads(self.snapshots[self.order[0]]))
        for i in range(1, len(self.order)):
            acc = jd.patch(acc, self.deltas[self.order[i]], load=True, dump=True)
            eq = json.loads(json.dumps(json.loads(acc))) == json.loads(json.dumps(json.loads(self.snapshots[self.order[i]])))

            print(f"Acc == Snapshot {i}: {eq}, similarity: {jd.similarity(acc, self.snapshots[self.order[i]], load=True)}")
            if not eq:
                print("Writing initial snapshot to json_a.json")
                with open("json_a.json", "w") as f:
                    json.dump(json.loads(self.snapshots[self.order[i]]), f, indent=4)
                    # f.write(snapshot_2)
                    # json.dump(json.loads(snapshot_2), f, indent=4)

                print("Writing reconstructed snapshot to json_b.json")
                with open("json_b.json", "w") as f:
                    json.dump(json.loads(acc), f, indent=4)
                    # f.write(snapshot2_reconstructed)
                    # json.dump(json.loads(snapshot2_reconstructed), f, indent=4)



# path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites.db"
path = "/home/alisot2000/Documents/01_ReposNCode/eth-video-indexer/scripts/seq_sites_hash_test.db"

if __name__ == "__main__":
    t = Test(path)
    t.get_snapshot()

    bigger_test= False
    if bigger_test:

        # snapshot_1 = json.loads(t.snapshots[t.order[0]])
        # snapshot_2 = json.loads(t.snapshots[t.order[1]])
        #
        # delta2 = jd.diff(snapshot_1, snapshot_2)
        # print(delta2)
        # snapshot2_reconstructed = jd.patch(snapshot_1, delta2)
        # print("Reconstructed  == Actual: ", snapshot2_reconstructed == snapshot_2)

        snapshot_1 = t.snapshots[t.order[0]]
        snapshot_2 = t.snapshots[t.order[1]]

        delta2 = jd.diff(snapshot_1, snapshot_2, load=True, dump=True)
        print(delta2)
        snapshot2_reconstructed = jd.patch(snapshot_1, delta2, load=True, dump=True)
        a = json.loads(snapshot2_reconstructed)
        b = json.loads(snapshot_2)
        a_hash = hash(a)
        b_hash = hash(b)
        print("Reconstructed  == Actual: ", a == b)
        print("Similarity: ", jd.similarity(snapshot_2, snapshot2_reconstructed, load=True))
        print(snapshot_2)
        print(snapshot2_reconstructed)

        # print("Writing initial snapshot to json_0.json")
        # with open("json_0.json",  "w") as f:
        #     json.dump(json.loads(t.snapshots[t.order[0]]), f, indent=4)

        print("Writing initial snapshot to json_a.json")
        with open("json_a.json",  "w") as f:
            # json.dump(json.loads(t.snapshots[t.order[1]]), f, indent=4)
            f.write(snapshot_2)
            # json.dump(json.loads(snapshot_2), f, indent=4)

        print("Writing reconstructed snapshot to json_b.json")
        with open("json_b.json", "w") as f:
            # json.dump(json.loads(snapshot2_reconstructed), f, indent=4)
            f.write(snapshot2_reconstructed)
            # json.dump(json.loads(snapshot2_reconstructed), f, indent=4)

    t.get_deltas()
    t.test_deltas()

