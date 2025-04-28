import time
import random
import argparse
import matplotlib.pyplot as plt
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import Query

def run_tests( data_size, iterations, range_size):
    ''' Choose the db and the index it has'''    
    db = IndexedTinyDB('db_100MB.json', sort_keys=True, indent=4, separators=(',', ': '))
    # Ensure fresh indexing
    db.create_index("$.user.age", "age", "NUMERIC")
    db.create_index("$.user.id", "id", "TEXT")
    db.create_index("$.user.data.first", "first", "NUMERIC")
    
    available_indexes = list(db.index_manager.index_specs.keys())

    def average_time(times):
        return sum(times) / len(times) if times else float('inf')

    # ----------------------------
    # Test 1: Range Queries (B+ Tree Index vs Full Scan)
    # ----------------------------
    print("=== Test 1: Range Queries ===")
    range_full_scan_times = []
    range_index_times = []
    full_scan_range_results = None
    index_range_results = None
    RANGE_SIZE = range_size
    NULL_COUNTER = 0


    def test_func(val, m, n):
        return m <= val < n

    for _ in range(iterations):
        alias = random.choice(available_indexes)
        _, index_type, _, _ = db.index_manager.index_specs[alias]

        if index_type == "TEXT":
            rand_val = random.randint(0, data_size)  # Adjust based on your data range.
            low = f'user_{rand_val:06}'
            high = f'user_{(rand_val + RANGE_SIZE):06}'
        elif index_type == "NUMERIC":
            low = random.randint(0, data_size)  # Adjust based on your data range.
            high = low + RANGE_SIZE

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

        if (full_scan_range_results != index_range_results):
            print(alias, low, high)
            print(full_scan_range_results)
            print(index_range_results)
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
    print("null percentage:", NULL_COUNTER / iterations)

    return {
        'avg_range_full_scan': avg_range_full_scan,
        'avg_range_index': avg_range_index,
        'improvement_range': improvement_range
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', type=int, default=100, help='queries per test')
    args = parser.parse_args()

    ranges = [100, 1000, 10000, 100000]
    
    results = []

    for range_size in ranges:
        metrics = run_tests(500000, args.i, range_size)
        metrics['range_size'] = range_size
        results.append(metrics)

    # Extract arrays
    sizes = [r['range_size'] for r in results]
    imp_range = [r['improvement_range'] for r in results]
    full_range = [r['avg_range_full_scan'] for r in results]
    idx_range = [r['avg_range_index'] for r in results]

    # Plot 1: Improvement %
    plt.figure()
    plt.plot(sizes, imp_range, marker='s', label='Range Query')
    plt.xscale('log')
    plt.xlabel('Range Size')
    plt.ylabel('Improvement (%)')
    plt.title('Index Improvement vs Range Size')
    plt.legend()

    # Plot 2: Query Speed
    plt.figure()
    plt.plot(sizes, full_range, marker='s', linestyle='--', label='Full Scan Range')
    plt.plot(sizes, idx_range, marker='s', label='Indexed Range')
    plt.xscale('log')
    plt.xlabel('Range Size')
    plt.ylabel('Average Query Time (s)')
    plt.title('Range Query Time vs Range Size')
    plt.legend()

    plt.show()

if __name__ == '__main__':
    main()
