import time
import random
import argparse
import matplotlib.pyplot as plt
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import Query

def run_tests(db_path, data_size, iterations, range_upper):
    db = IndexedTinyDB(db_path, sort_keys=True, indent=4, separators=(',', ': '))
    # Ensure fresh indexing
    db.create_index("$.user.age", "age", "NUMERIC")
    db.create_index("$.user.id", "id", "TEXT")
    db.create_index("$.user.data.first", "first", "NUMERIC")

    available_indexes = list(db.index_manager.index_specs.keys())

    def average_time(times):
        return sum(times) / len(times) if times else float('inf')

    # ----------------------------
    # Test 1: Exact Match (Hash Index vs Full Scan)
    # ----------------------------
    print("\n=== Test 1: Exact Match Queries ===")
    hash_full_scan_times = []
    hash_index_times = []
    full_scan_results = None
    index_results = None
    NULL_COUNTER = 0

    for _ in range(iterations):
        alias = random.choice(available_indexes)
        _, index_type, _, _ = db.index_manager.index_specs[alias]

        if index_type == "TEXT":
            rand_val = random.randint(0, data_size)  # Adjust based on your data range.
            query_value = f'user_{rand_val:06}'
        elif index_type == "NUMERIC":
            query_value = random.randint(0, data_size)  # Adjust based on your data range.


        # Indexed query using the hash index.
        start = time.time()
        index_results = db.search((alias, query_value))
        elapsed = time.time() - start
        hash_index_times.append(elapsed)
        
        # Full scan using a Query object (fallback: not using the index)
        query_obj = Query()
        start = time.time()
        if alias == 'age':
            full_scan_results = db.search(query_obj.user.age == query_value)
        elif alias == 'id':
            full_scan_results = db.search(query_obj.user.id == query_value)
        elif alias == 'first':
            full_scan_results = db.search(query_obj.user.data.first == query_value)
        
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

    # Calculate averages.
    avg_full_scan = average_time(hash_full_scan_times)
    avg_hash_index = average_time(hash_index_times)
    improvement_hash = ((avg_full_scan - avg_hash_index) / avg_full_scan * 100) if avg_full_scan > 0 else 0

    print(f"Exact match full scan: Avg Time = {avg_full_scan:.6f} sec" )
    print(f"Exact match indexed (Hash): Avg Time = {avg_hash_index:.6f} sec")
    print(f"Improvement: {improvement_hash:.2f}%\n")



    # ----------------------------
    # Test 2: Range Queries (B+ Tree Index vs Full Scan)
    # ----------------------------
    print("=== Test 2: Range Queries ===")
    range_full_scan_times = []
    range_index_times = []
    full_scan_range_results = None
    index_range_results = None

    def test_func(val, m, n):
        return m <= val < n

    for _ in range(iterations):
        RANGE_SIZE = random.randint(2, range_upper)
        
        alias = random.choice(available_indexes)
        _, index_type, _, _ = db.index_manager.index_specs[alias]

        if index_type == "TEXT":
            rand_val = random.randint(0, data_size)  # Adjust based on your data range.
            low = f'user_{rand_val:06}'
            high = f'user_{(rand_val + RANGE_SIZE):06}'
        elif index_type == "NUMERIC":
            low = random.randint(0, data_size)  # Adjust based on your data range.
            high = low + RANGE_SIZE


        # print(alias, low, high)
        # Indexed range query using B+ tree index.
        start = time.time()
        index_range_results = db.search({alias: (low, high)})
        elapsed = time.time() - start
        range_index_times.append(elapsed)

        # Full scan range query using a Query object and a test function.
        query_obj = Query()
        start = time.time()
        if alias == 'age':
            full_scan_range_results = db.search(query_obj.user.age.test(test_func, low, high))
        elif alias == 'id':
            full_scan_range_results = db.search(query_obj.user.id.test(test_func, low, high))
        elif alias == 'first':
            full_scan_range_results = db.search(query_obj.user.data.first.test(test_func, low, high))
        elapsed = time.time() - start
        range_full_scan_times.append(elapsed)

        # print(full_scan_range_results)
        # print(index_range_results)
        if (full_scan_range_results != index_range_results):
            raise ValueError("Unmatched B+ tree and full scan result")

        if not full_scan_range_results :
            NULL_COUNTER += 1

    # Calculate averages.
    avg_range_full_scan = average_time(range_full_scan_times)
    avg_range_index = average_time(range_index_times)
    improvement_range = ((avg_range_full_scan - avg_range_index) / avg_range_full_scan * 100) if avg_range_full_scan > 0 else 0

    print(f"Range query full scan: Avg Time = {avg_range_full_scan:.6f} sec")
    print(f"Range query indexed (B+ Tree): Avg Time = {avg_range_index:.6f} sec")
    print(f"Improvement: {improvement_range:.2f}%\n")

    print("null counter:", NULL_COUNTER)
    print("null percentage:", NULL_COUNTER / (iterations * 2))


    return {
        'avg_full_scan': avg_full_scan,
        'avg_hash_index': avg_hash_index,
        'improvement_exact': improvement_hash,
        'avg_range_full_scan': avg_range_full_scan,
        'avg_range_index': avg_range_index,
        'improvement_range': improvement_range
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', type=int, default=100, help='queries per test')
    parser.add_argument('-r', type=int, default=100, help='max range size')
    args = parser.parse_args()

    # List of (file, size_in_MB)
    dbs = [
        # ('db_100KB.json', 0.1),
        # ('db_1MB.json', 1),
        ('db_3node_10MB.json', 10)
        # ('db_100MB.json', 100)
    ]

    results = []
    for path, size in dbs:
        data_size = int(500 * (size / 0.1))
        metrics = run_tests(path, data_size, args.i, args.r)
        metrics['size_mb'] = size
        results.append(metrics)

    # Extract arrays
    sizes = [r['size_mb'] for r in results]
    imp_exact = [r['improvement_exact'] for r in results]
    imp_range = [r['improvement_range'] for r in results]
    full_exact = [r['avg_full_scan'] for r in results]
    idx_exact = [r['avg_hash_index'] for r in results]
    full_range = [r['avg_range_full_scan'] for r in results]
    idx_range = [r['avg_range_index'] for r in results]

    # Plot 1: Improvement %
    plt.figure()
    plt.plot(sizes, imp_exact, marker='o', label='Exact Match')
    plt.plot(sizes, imp_range, marker='s', label='Range Query')
    plt.xscale('log')
    plt.xlabel('Database Size (MB)')
    plt.ylabel('Improvement (%)')
    plt.title('Index Improvement vs Database Size')
    plt.legend()

    # Plot 2: Query Speed
    plt.figure()
    plt.plot(sizes, full_exact, marker='o', linestyle='--', label='Full Scan Exact')
    plt.plot(sizes, idx_exact, marker='o', label='Indexed Exact')
    plt.plot(sizes, full_range, marker='s', linestyle='--', label='Full Scan Range')
    plt.plot(sizes, idx_range, marker='s', label='Indexed Range')
    plt.xscale('log')
    plt.xlabel('Database Size (MB)')
    plt.yscale('log')
    plt.ylabel('Average Query Time (s)')
    plt.title('Query Time vs Database Size')
    plt.legend()

    plt.show()

if __name__ == '__main__':
    main()
