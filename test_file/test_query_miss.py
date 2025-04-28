import time
import random
import argparse
import matplotlib.pyplot as plt
from tinydb_test.indexed_tinydb import IndexedTinyDB
from tinydb_test import Query


def run_miss_test(db_path, data_size, iterations, miss_rate, range_size):
    """
    Run exact-match and range queries with a given miss_rate.
    Existing values: [0, data_size]
    Miss values: [missing_low, missing_high]
    Returns averages: full_exact, idx_exact, full_range, idx_range
    """
    # Initialize DB and ensure indexes
    db = IndexedTinyDB(db_path, sort_keys=True, indent=4, separators=(',', ': '))
    db.create_index("$.user.age", "age", "NUMERIC")
    # db.create_index("$.user.fuck", "id", "TEXT")
    # db.create_index("$.user.data.first", "first", "NUMERIC")

    alias = "age"
    exact_full_times = []
    exact_index_times = []
    range_full_times = []
    range_index_times = []
    NULL_COUNTER = 0
    query_obj = Query()

    for _ in range(iterations):
        # decide hit or miss
        if random.random() < miss_rate:
            qv = data_size + random.randint(0, data_size)

        else:
            qv = random.randint(0, data_size)

        # Exact match: indexed vs full scan
        start = time.time()
        index_results = db.search((alias, qv))
        elapsed = time.time() - start
        exact_index_times.append(elapsed)

        start = time.time()
        full_scan_results = db.search(query_obj.user.age == qv)
        elapsed = time.time() - start
        exact_full_times.append(elapsed)

        if (full_scan_results != index_results):
            raise ValueError("Unmatched B+ tree and full scan result")

        if not full_scan_results :
            NULL_COUNTER += 1
        
        # Range query: [qv, qv + range_size)
        low = qv
        high = qv + range_size
        start = time.time()
        index_range_results = db.search({alias: (low, high)})
        elapsed = time.time() - start
        range_index_times.append(elapsed)

        start = time.time()
        full_scan_range_results = db.search(query_obj.user.age.test(lambda v, a, b: a <= v < b, low, high))
        elapsed = time.time() - start
        range_full_times.append(elapsed)

        if (full_scan_range_results != index_range_results):
            raise ValueError("Unmatched B+ tree and full scan result")

        if not full_scan_range_results :
            NULL_COUNTER += 1


   

    # Compute averages
    avg_exact_full = sum(exact_full_times) / len(exact_full_times)
    avg_exact_idx  = sum(exact_index_times) / len(exact_index_times)
    improvement_hash = ((avg_exact_full - avg_exact_idx) / avg_exact_full * 100) if avg_exact_full > 0 else 0

    avg_range_full = sum(range_full_times) / len(range_full_times)
    avg_range_idx  = sum(range_index_times) / len(range_index_times)
    improvement_range = ((avg_range_full - avg_range_idx) / avg_range_full * 100) if avg_range_full > 0 else 0

    print("\n=== Test 1: Exact Match Queries ===")
    print(f"Exact match full scan: Avg Time = {avg_exact_full:.6f} sec" )
    print(f"Exact match indexed (Hash): Avg Time = {avg_exact_idx:.6f} sec")
    print(f"Improvement: {improvement_hash:.2f}%\n")
    print("=== Test 2: Range Queries ===")
    print(f"Range query full scan: Avg Time = {avg_range_full:.6f} sec")
    print(f"Range query indexed (B+ Tree): Avg Time = {avg_range_idx:.6f} sec")
    print(f"Improvement: {improvement_range:.2f}%\n")
    
    print("=== Current Iter Miss Rate ===")
    print(f"target miss rate:{miss_rate}, iterations:{iterations}")
    print(f"actual miss rate:{NULL_COUNTER / (iterations * 2)}\n")

    return avg_exact_full, avg_exact_idx, avg_range_full, avg_range_idx


def main():
    parser = argparse.ArgumentParser(description="Test query speed vs miss rate.")
    parser.add_argument('-d', type=int, default=50000,
                        help='max existing value (exclusive)')
    parser.add_argument('-i', type=int, default=100,
                        help='number of queries per miss-rate test')
    parser.add_argument('-m', type=float, nargs=6,
                        default=[0.0, 0.25, 0.5, 0.75, 0.9, 0.95],
                        help='list of 5 miss rates to test')
    parser.add_argument('-g', type=int, default=100,
                        help='range size for range queries')
    parser.add_argument('-f', type=str, default='db_miss_10MB.json',
                        help='path to JSON DB file')
    args = parser.parse_args()

    data_size   = args.d
    iterations  = args.i
    miss_rates  = args.m
    range_size  = args.g
    db_path     = args.f

    # result accumulators
    exact_full = []
    exact_idx  = []
    range_full = []
    range_idx  = []
    impr_exact = []
    impr_range = []

    for mr in miss_rates:
        ef, ei, rf, ri = run_miss_test(db_path, data_size, iterations, mr, range_size)
        exact_full.append(ef)
        exact_idx.append(ei)
        range_full.append(rf)
        range_idx.append(ri)
        impr_exact.append(((ef - ei) / ef * 100) if ef>0 else 0)
        impr_range.append(((rf - ri) / rf * 100) if rf>0 else 0)
        print(f" Exact: full={ef:.6f}s, idx={ei:.6f}s, impr={impr_exact[-1]:.2f}%")
        print(f" Range: full={rf:.6f}s, idx={ri:.6f}s, impr={impr_range[-1]:.2f}%")

    # Plot 1: Improvement vs Miss Rate
    plt.figure()
    plt.plot(miss_rates, impr_exact, marker='o', label='Exact Improvement (%)')
    plt.plot(miss_rates, impr_range, marker='s', label='Range Improvement (%)')
    plt.xlabel('Miss Rate')
    plt.ylabel('Improvement (%)')
    plt.title('Miss Rate vs Query Improvement')
    plt.legend()

    # Plot 2: Query Speed vs Miss Rate
    plt.figure()
    plt.plot(miss_rates, exact_full, marker='o', linestyle='--', label='Full Scan Exact')
    plt.plot(miss_rates, exact_idx,  marker='o', label='Indexed Exact')
    plt.plot(miss_rates, range_full, marker='s', linestyle='--', label='Full Scan Range')
    plt.plot(miss_rates, range_idx,  marker='s', label='Indexed Range')
    plt.xlabel('Miss Rate')
    plt.ylabel('Avg Query Time (s)')
    plt.title('Miss Rate vs Query Speed')
    plt.legend()

    plt.show()

if __name__ == '__main__':
    main()
