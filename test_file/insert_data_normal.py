import time
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import where, Query

# Initialize database
db = IndexedTinyDB('db.json', sort_keys=True, indent=4, separators=(',', ': '))

"""The index and list are store in disk. We can create/reuse like this"""
db.create_index("$.user.age", "age", "NUMERIC")
db.create_index("$.user.id", "id", "TEXT")
db.create_index("$.user.data.first", "first", "NUMERIC")


""" Add any data as you want """
db.insert_multiple([{"user":{'id': f'user_{i:06}', 'age': i}} for i in range(30000)] + 
                   [{"user":{'id': f'user_{i:06}', 'age': i}} for i in range(15000)] +
                   [{"user":{
                    'hello': f'user_{i:06}', 
                    'age': i, 
                    'data': {
                        'first': i,
                        'second': i + i
                    }
                }
            } for i in range(23000, 50000)])


# ----------------------------
# Printing out all the indexes and list (to verify it works)
# ----------------------------
# i = 0
# for alias, (_, index_type, _, pointer_store) in db.index_manager.index_specs.items():
#     print(f"index: {alias}")
#     for key, value  in pointer_store.items():
#         if index_type == "TEXT":
#             key = key.decode('utf-8').rstrip('\x00')
#         else:
#             key = int.from_bytes(key, byteorder='big', signed=True)
#         # if (len(value) >= 2):
#         print("key: ", key)
#         print(value)

#     # if i == 2:
#     #     break
#     # i+=1
#     print()

# ----------------------------
# Testing hash index compare to original index-less tinydb
# ----------------------------

# query_age = Query()
# start_time = time.time()
# print(db.search(query_age.user.id == 'user_2'))
# elapsed = time.time() - start_time
# print(f"Original Elapsed time: {elapsed} seconds")

# """ Right now only supports equal (age == 2) """
# start_time = time.time()
# db.search(('level_1', 'user_25'))  # Using Hash Index
# elapsed = time.time() - start_time
# print(f"Elapsed time: {elapsed} seconds")

# print()



# ----------------------------
# Testing b+ tree index compare to original index-less tinydb
# ----------------------------

# def test_func(val, m, n):
#     return m <= val < n
# query_age = Query()

# start_time = time.time()
# print(db.search(query_age.user.fuck.test(test_func, "user_0007", "user_0010")))
# elapsed = time.time() - start_time
# print(f"Original Elapsed time: {elapsed} seconds")

# """ Right now only supports range (2 <= age < 10) """
# start_time = time.time()
# print(db.search({'id': ("user_0007", "user_0010")}))  # Using B+ Tree Index
# # print(db.search(('fuck', "user_0028")))  # Using Hash Tree Index
# elapsed = time.time() - start_time
# print(f"Elapsed time: {elapsed} seconds")


# print()

# ----------------------------
# Hash index test
# ----------------------------
# query_age = Query()
# start_time = time.time()
# db.search(query_age.user.age == 2400)
# elapsed = time.time() - start_time
# print(f"Original Elapsed time: {elapsed} seconds")

# """ Right now only supports equal (age == 2) """
# start_time = time.time()
# db.search(('age', 2400))  # Using Hash Index
# elapsed = time.time() - start_time
# print(f"Elapsed time: {elapsed} seconds")


