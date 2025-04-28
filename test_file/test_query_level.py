import time
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import Query
import random
import argparse
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description='Performance testing for different index depths.')
    parser.add_argument('-i', type=int, default=100,
                        help='number of random queries per test (default: 100)')
    parser.add_argument('-r', type=int, default=100,
                        help='the max range size for range query (default: 100)')
    parser.add_argument('-d', type=int, default=15000,
                        help='max data value in DB (default: 5000)')
    args = parser.parse_args()

    ITERATIONS = args.i
    RANGE_UPPER = args.r
    DATA_SIZE = args.d
    DB_FILE = 'db_level_fff.json'
    NULL_COUNTER = 0

    # Define levels and their JSONPaths
    test_levels = [
        (1, '$.data.level_1', 'TEXT'),
        (3, '$.data.dummy_1.dummy_1.level_3', 'NUMERIC'),
        (5, '$.data.dummy_2.dummy_2.dummy_2.dummy_2.level_5', 'NUMERIC'),
        (7, '$.data.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.level_7', 'NUMERIC'),
        (9, '$.data.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.level_9', 'NUMERIC'),
        (11, '$.data.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.level_11', 'NUMERIC'),
    ]

    db = IndexedTinyDB(DB_FILE, sort_keys=True, indent=4, separators=(',', ': '))

    results = {}
    
    for depth, jsonpath, index_type in test_levels:
        alias = f'level_{depth}'
        db.create_index(jsonpath, alias, index_type)

        print(f"\n--- Testing {alias} at JSONPath {jsonpath} ---\n")
        print("=== Test 1: Exact Match Queries ===")
        # Initialize DB and create NUMERIC index for this level

        # --- Exact Match Test (Hash vs Full Scan) ---
        hash_full_scan_times = []
        hash_index_times = []
        for _ in range(ITERATIONS):
            if index_type == "TEXT":
                rand_val = random.randint(0, DATA_SIZE)  # Adjust based on your data range.
                query_value = f'user_{rand_val:06}'
            elif index_type == "NUMERIC":
                query_value = random.randint(0, DATA_SIZE)  # Adjust based on your data range.

            # Indexed query using the hash index.
            start = time.time()
            index_results = db.search((alias, query_value))
            elapsed = time.time() - start
            hash_index_times.append(elapsed)
            
            # Full scan using a Query object (fallback: not using the index)
            query_obj = Query()
            start = time.time()
            if depth == 1:
                full_scan_results = db.search(query_obj.data.level_1 == query_value)
            elif depth == 3:
                full_scan_results = db.search(query_obj.data.dummy_1.dummy_1.level_3 == query_value)
            elif depth == 5:
                full_scan_results = db.search(query_obj.data.dummy_2.dummy_2.dummy_2.dummy_2.level_5 == query_value)
            elif depth == 7 :
                full_scan_results = db.search(query_obj.data.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.level_7 == query_value)
            elif depth == 9 :
                full_scan_results = db.search(query_obj.data.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.level_9 == query_value)
            elif depth == 11 : 
                full_scan_results = db.search(query_obj.data.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.level_11 == query_value)
            
            elapsed = time.time() - start
            hash_full_scan_times.append(elapsed)

            if (index_results != full_scan_results):
                print(alias, query_value)
                print(index_results)
                print("--------------------------")
                print(full_scan_results)
                raise ValueError("Unmatched hash and full scan result")

            if not full_scan_results :
                NULL_COUNTER += 1

        avg_full = sum(hash_full_scan_times) / len(hash_full_scan_times)
        avg_hash = sum(hash_index_times) / len(hash_index_times)
        impr_exact = ((avg_full - avg_hash) / avg_full * 100) if avg_full > 0 else 0

        print(f"Exact match full scan: Avg Time = {avg_full:.6f} sec" )
        print(f"Exact match indexed (Hash): Avg Time = {avg_hash:.6f} sec")
        print(f"Improvement: {impr_exact:.2f}%\n")
        # print(f"Exact match: full={avg_full:.6f}s, hash={avg_hash:.6f}s, improvement={impr_exact:.2f}%\n")


        # --- Range Query Test (B+ Tree vs Full Scan) ---
        print("=== Test 2: Range Queries ===")
        range_full_scan_times = []
        range_index_times = []
        def test_func(val, m, n):
            return m <= val < n

        for _ in range(ITERATIONS):
            RANGE_SIZE = random.randint(2, RANGE_UPPER)
            
            if index_type == "TEXT":
                rand_val = random.randint(0, DATA_SIZE)  # Adjust based on your data range.
                low = f'user_{rand_val:06}'
                high = f'user_{(rand_val + RANGE_SIZE):06}'
            elif index_type == "NUMERIC":
                low = random.randint(0, DATA_SIZE)  # Adjust based on your data range.
                high = low + RANGE_SIZE

            # Indexed range query using B+ tree index.
            start = time.time()
            index_range_results = db.search({alias: (low, high)})
            elapsed = time.time() - start
            range_index_times.append(elapsed)

            # Full scan range query using a Query object and a test function.
            query_obj = Query()
            start = time.time()
            if depth == 1:
                full_scan_range_results = db.search(query_obj.data.level_1.test(test_func, low, high))
            elif depth == 3:
                full_scan_range_results = db.search(query_obj.data.dummy_1.dummy_1.level_3.test(test_func, low, high))
            elif depth == 5:
                full_scan_range_results = db.search(query_obj.data.dummy_2.dummy_2.dummy_2.dummy_2.level_5.test(test_func, low, high))
            elif depth == 7 :
                full_scan_range_results = db.search(query_obj.data.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.dummy_3.level_7.test(test_func, low, high))
            elif depth == 9 :
                full_scan_range_results = db.search(query_obj.data.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.dummy_4.level_9.test(test_func, low, high))
            elif depth == 11 : 
                full_scan_range_results = db.search(query_obj.data.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.dummy_5.level_11.test(test_func, low, high))
            elapsed = time.time() - start
            range_full_scan_times.append(elapsed)

            if (full_scan_range_results != index_range_results):
                raise ValueError("Unmatched B+ tree and full scan result")
            if not full_scan_range_results :
                NULL_COUNTER += 1
        avg_rfull = sum(range_full_scan_times) / len(range_full_scan_times)
        avg_bpt = sum(range_index_times) / len(range_index_times)
        impr_range = ((avg_rfull - avg_bpt) / avg_rfull * 100) if avg_rfull > 0 else 0

        print(f"Range query full scan: Avg Time = {avg_rfull:.6f} sec")
        print(f"Range query indexed (B+ Tree): Avg Time = {avg_bpt:.6f} sec")
        print(f"Improvement: {impr_range:.2f}%\n")


        print("null counter:", NULL_COUNTER)
        print("null percentage:", NULL_COUNTER / (ITERATIONS * 2))

        # Store results
        results[depth] = {
            'exact_full': avg_full,
            'hash_index': avg_hash,
            'impr_exact': impr_exact,
            'range_full': avg_rfull,
            'bpt_index': avg_bpt,
            'impr_range': impr_range,
        }

    # --- Plotting ---
    depths = sorted(results.keys())
    exact_impr = [results[d]['impr_exact'] for d in depths]
    range_impr = [results[d]['impr_range'] for d in depths]

    # Plot 1: Improvement vs Level
    plt.figure()
    plt.plot(depths, exact_impr, marker='o', label='Exact Match Improvement (%)')
    plt.plot(depths, range_impr, marker='s', label='Range Query Improvement (%)')
    plt.xlabel('Index Level (depth)')
    plt.ylabel('Improvement (%)')
    plt.title('Index Level vs. Query Improvement')
    plt.legend()
    plt.tight_layout()

    # Plot 2: Query Speed vs Level
    full_exact = [results[d]['exact_full'] for d in depths]
    idx_exact = [results[d]['hash_index'] for d in depths]
    full_range = [results[d]['range_full'] for d in depths]
    idx_range = [results[d]['bpt_index'] for d in depths]

    plt.figure()
    plt.plot(depths, full_exact, marker='o', linestyle='--', label='Full Scan Exact')
    plt.plot(depths, idx_exact, marker='o', label='Indexed Exact')
    plt.plot(depths, full_range, marker='s', linestyle='--', label='Full Scan Range')
    plt.plot(depths, idx_range, marker='s', label='Indexed Range')
    plt.xlabel('Index Level (depth)')
    plt.ylabel('Avg Query Time (s)')
    plt.title('Index Level vs. Query Speed')
    plt.legend()
    plt.tight_layout()

    plt.show()

if __name__ == '__main__':
    main()
