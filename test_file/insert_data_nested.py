import time
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import where, Query
from tinydb_test.indexed_tinydb import IndexedTinyDB
import random

MAX_NUM = 3000

def build_nested_payload(i: int, level: int) -> dict:
    """
    Build a dict of the form:
      { 'data': {  # top‐level
          # if level==1, just 'level_1': 'user_000123'
          # else dummy_d nested (level–1) times, ending in 'level_{level}': i
      } }
    where d = (level–1)//2.
    """
    if level == 1:
        # flat case
        return {"data": {"level_1": f"user_{random.randint(0, MAX_NUM):06}"}}
    # compute which dummy_X to repeat
    d = (level - 1) // 2
    dummy_key = f"dummy_{d}"

    # build the nesting
    payload = {}
    cur = payload
    for _ in range(level - 1):
        cur[dummy_key] = {}
        cur = cur[dummy_key]
    # finally insert the level_N leaf
    cur[f"level_{level}"] = random.randint(0, MAX_NUM)

    return {"data": payload}


if __name__ == "__main__":
    db = IndexedTinyDB("db_up_to_lvl11.json", sort_keys=True, indent=4)

    # --- 2) create indexes for levels 1,3,5,7,9,11 ---
    for lvl in [1, 3, 5, 7, 9, 11]:
        # build the JSONPath string
        if lvl == 1:
            path = "$.data.level_1"
            idx_type = "TEXT"
        else:
            d = (lvl - 1) // 2
            segs = ["data"] + [f"dummy_{d}"] * (lvl - 1) + [f"level_{lvl}"]
            path = "$." + ".".join(segs)
            idx_type = "NUMERIC"
        alias = f"level_{lvl}"
        db.create_index(path, alias, idx_type)

    # prepare one big list of docs for levels 1,3,5,7,9,11
    docs = []
    for lvl in [1, 3, 5, 7, 9, 11]:
        # here we insert 30 000 docs per level; adjust the range() if you like
        docs.extend(build_nested_payload(i, lvl) for i in range(MAX_NUM))

    # this will crank through ~180 000 documents in one shot
    db.insert_multiple(docs)



    # index_results = db.search(('level_7', 2529))
    for alias, (_, index_type, _, pointer_store) in db.index_manager.index_specs.items():
        print(f"index: {alias}")
        for key, value  in pointer_store.items():
            if index_type == "TEXT":
                key = key.decode('utf-8').rstrip('\x00')
            else:
                key = int.from_bytes(key, byteorder='big', signed=True)
            if (len(value) >= 5):
                print("key: ", key)
                print(value)

        print()



