import os
from bplustree import BPlusTree
from bplustree.serializer import Serializer
from .utils import str_to_bytes, int_to_bytes
import shelve
from pathlib import Path

# Persistence functions for the pointer store using shelve.
def pointer_store_path(index_dir, file_name):
    return os.path.join(index_dir, file_name)

def load_pointer_store(store_path):
    """Load the pointer store from disk; keys are stored as hex strings."""
    if not os.path.exists(f"{store_path}.db"):
        return {}
    with shelve.open(store_path, flag='r') as shelf:
        return {bytes.fromhex(k): shelf[k] for k in shelf}

def save_pointer_store(pointer_store, store_path):
    """Save the pointer store to disk; convert bytes keys to hex strings."""
    with shelve.open(store_path, flag='n') as shelf:
        for pointer, doc_ids in pointer_store.items():
            shelf[pointer.hex()] = doc_ids

class RawBytesSerializer(Serializer):
    def serialize(self, obj, key_size=8):
        # If obj is already bytes and the length matches, return it.
        if isinstance(obj, bytes):
            if len(obj) == key_size:
                return obj
            elif len(obj) > key_size:
                return obj[:key_size]
            else:
                return obj.ljust(key_size, b'\x00')
        # Optionally, you could handle other types here
        raise TypeError("Expected bytes, got: {}".format(type(obj)))
    
    def deserialize(self, data):
        # For simplicity, return the data as-is.
        return data

class IndexManager:
    def __init__(self, path: str, index_dir='indexes', list_dir='posting_list'):
        db_name = Path(path).stem
        self.index_dir = index_dir + '/' + db_name
        self.list_dir = list_dir + '/' + db_name
        os.makedirs(self.index_dir, exist_ok=True)  # Ensure the directory exists
        os.makedirs(self.list_dir, exist_ok=True)  # Ensure the directory exists

        self.index_specs = {}
        self.max_index_text_len = 13

    def create_index(self, jsonpath: str, alias: str, index_type: str) -> None:
        """
        Create an index on a specific JSON field before inserting documents.
        Syntax example:
            db.create_index("$.user.a", "user", "TEXT")
        
        Spec:
        1. User must decide which key to index.
        2. Do not index if the value is a dict.
        3. For TEXT: build an inverted index.
        4. For NUMERIC: build a sorted index (simulated B+ tree).
        """
        if index_type.upper() != "TEXT" and index_type.upper() != "NUMERIC":
            raise ValueError("Unsupported index type. Use TEXT or NUMERIC.")

        if alias not in self.index_specs:
            self.index_specs[alias] = {}
        if not self.index_specs[alias]:
            bplustree_index = self.create_bplustree(alias, jsonpath, index_type.upper())
            pointer_store = load_pointer_store(pointer_store_path(self.list_dir, f"doc_id_list_{alias}_{jsonpath}"))
            self.index_specs[alias] = (jsonpath, index_type.upper(), bplustree_index, pointer_store)
        else:
            print("Index already exist.")
            return

        print(f"Index '{alias}' created with type '{index_type}' on path {jsonpath}.")


    def create_bplustree(self, alias, key, index_type):
        """Create a B+Tree index for a given key if it doesn't exist."""
        index_path = os.path.join(self.index_dir, f'btree_{alias}_{key}.db')
        if index_type == "TEXT":
            return BPlusTree(index_path, order=3, serializer=RawBytesSerializer(), key_size=self.max_index_text_len)
        else:
            return BPlusTree(index_path, order=3)

    def update_index(self, alias, key_bytes, doc_id):
        """
        Update the B+Tree index for the given alias and key.
        Instead of storing the document id directly, store a pointer.
        Update the external pointer_store accordingly and persist it.
        """
        if alias not in self.index_specs:
            return

        jsonpath, index_type, bplus_tree, pointer_store = self.index_specs[alias]
        try:
            # Attempt to get the pointer for the key.
            pointer = bplus_tree.get(key_bytes)
        except KeyError:
            pointer = None

        if pointer is None:
            # Generate a new pointer.
            if index_type == "NUMERIC":
                pointer = int_to_bytes(key_bytes)
            else:
                pointer = key_bytes

            # Initialize pointer_store entry.
            pointer_store[pointer] = [doc_id]
            # Insert into the B+Tree index.
            bplus_tree.insert(key_bytes, pointer)
        else:
            # Update pointer_store.
            if pointer not in pointer_store:
                pointer_store[pointer] = []
            if doc_id not in pointer_store[pointer]:
                pointer_store[pointer].append(doc_id)

        # Persist the pointer_store to disk.
        save_pointer_store(pointer_store, pointer_store_path(self.list_dir, f"doc_id_list_{alias}_{jsonpath}"))

    def batch_update_index(self, alias: str, iterable):
        """
        Batch insert or update an entire index in one go.

        iterable must yield tuples (key_bytes, doc_id).  Keys will be
        grouped, pointer lists merged, and the B+Tree will be fed in
        ascending‐key order via its own .batch_insert() method.
        """
        if alias not in self.index_specs:
            raise KeyError(f"No such index: {alias}")

        jsonpath, index_type, bplus_tree, pointer_store = self.index_specs[alias]

        # 1) Build up a map: pointer → [new doc_ids]
        new_entries = {}
        for key_bytes, doc_id in iterable:
            # TEXT uses the raw bytes as pointer; NUMERIC packs the int into bytes
            pointer = key_bytes if index_type == "TEXT" else int_to_bytes(key_bytes)
            new_entries.setdefault(pointer, []).append(doc_id)

        # 2) Merge into the existing pointer_store (deduplicating)
        for pointer, doc_ids in new_entries.items():
            existing = pointer_store.get(pointer, [])
            # union of old and new
            pointer_store[pointer] = list(set(existing) | set(doc_ids))

        # 3) Prepare the B+Tree batch list: (key, pointer)
        batch_list = []
        for pointer in new_entries:
            if index_type == "NUMERIC":
                # unpack back to integer key
                key = int.from_bytes(pointer, byteorder='big', signed=True)
            else:
                key = pointer
            batch_list.append((key, pointer))

        # sort by key ascending (required by BPlusTree.batch_insert)
        batch_list.sort(key=lambda kv: kv[0])

        # 4) Do the single-transaction bulk insert
        bplus_tree.batch_insert(batch_list)

        # 5) Persist the updated pointer store
        store_path = pointer_store_path(
            self.list_dir,
            f"doc_id_list_{alias}_{jsonpath}"
        )
        save_pointer_store(pointer_store, store_path)


    def search_btree_range(self, alias, min_v, max_v):
        """Search for a range of values using B+Tree indexing."""
        if alias in self.index_specs:
            _, index_type, bplustree_index, pointer_store = self.index_specs[alias]
            
            if index_type == "NUMERIC":
                pointers = [value for _, value in bplustree_index.items(slice(min_v, max_v))]
            else:
                pointers = [value for _, value in bplustree_index.items(slice(str_to_bytes(min_v, self.max_index_text_len), str_to_bytes(max_v, self.max_index_text_len)))]

            doc_ids = []
            for pointer in pointers:
                for doc_id in pointer_store.get(pointer, []):
                    # doc_id = int.from_bytes(doc_id_bytes, byteorder='big', signed=True)
                    doc_ids.append(doc_id)
            return doc_ids
        
        return []
    
    def search_hash(self, alias, value):
        if alias in self.index_specs:
            _, index_type, _, pointer_store = self.index_specs[alias]
            # print(pointer_store)
            if index_type == "NUMERIC":
                return pointer_store.get(int_to_bytes(value), set())
            else:
                return pointer_store.get(str_to_bytes(value, self.max_index_text_len), set())
        return []
