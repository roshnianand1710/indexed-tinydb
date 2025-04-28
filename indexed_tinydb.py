from typing import (
    Iterable,
    List,
    Mapping
)

from tinydb_test import TinyDB
from .index_manager import IndexManager
from .utils import str_to_bytes

class IndexedTinyDB(TinyDB):
    def __init__(self, *args, **kwargs):
        """Initialize TinyDB with Index Manager."""
        super().__init__(*args, **kwargs)
        self.index_manager = IndexManager(*args)

    def create_index(self, jsonpath: str, alias: str, index_type: str) -> None:
        self.index_manager.create_index(jsonpath, alias, index_type)

    def update_index(self, value, alias, doc_id, index_type):
        if value is None or isinstance(value, dict):
            return

        if index_type == "TEXT":
            key_bytes = str_to_bytes(value, self.index_manager.max_index_text_len)
        elif index_type == "NUMERIC":
            key_bytes = value

        self.index_manager.update_index(alias, key_bytes, doc_id)


    def extract_by_jsonpath(self, doc: dict, path):
        if not path.startswith("$."):
            raise ValueError("Unsupported JSONPath format")
        parts = path[2:].split(".")
        value = doc
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None  # Path does not exist in document
        return value

    
    def insert(self, document: dict):
        """Insert a document and update indexes."""
        doc_id = self.table(self.default_table_name).insert(document)  # FIXED

        for alias, (jsonpath, index_type, _, _) in self.index_manager.index_specs.items():
            value = self.extract_by_jsonpath(document, jsonpath)
            
            # The indexed value has longer length than the max_index_text_len which may lead error when querying
            if value and index_type == 'TEXT' and len(value) > self.index_manager.max_index_text_len:
                self.table(self.default_table_name).remove(doc_ids=[doc_id])
                raise ValueError (f'Indexed value: {value} has length longer than max_index_text_len: {self.index_manager.max_index_text_len}' )
        
        # I'm lazy I don't want to figure it out a better way to implement this
        for alias, (jsonpath, index_type, _, _) in self.index_manager.index_specs.items():
            value = self.extract_by_jsonpath(document, jsonpath)
            self.update_index(value, alias, doc_id, index_type)
        
        return doc_id


    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """
        Insert many documents and update all indexes in one bulk operation.
        Loops are ordered doc → index for better locality.

        !! If the target json are not empty then you must use normal insert or else it will fail !!
        """
        # 1) Insert into TinyDB and get all new doc_ids
        doc_ids = self.table(self.default_table_name).insert_multiple(documents)

        # 2) Prepare a list of (key_bytes, doc_id) for each index alias
        pairs_by_alias = {
            alias: []
            for alias in self.index_manager.index_specs
        }

        # 3) For each new document, extract every indexed field
        for doc_id, doc in zip(doc_ids, documents):
            for alias, (jsonpath, index_type, _, _) in self.index_manager.index_specs.items():
                value = self.extract_by_jsonpath(doc, jsonpath)
                if value is None or isinstance(value, dict):
                    continue

                if index_type == "TEXT":
                    key_bytes = str_to_bytes(value, self.index_manager.max_index_text_len)
                else:  # "NUMERIC"
                    key_bytes = value  # already an int

                pairs_by_alias[alias].append((key_bytes, doc_id))

        # 4) Bulk‐update each index in one call
        for alias, pairs in pairs_by_alias.items():
            if pairs:
                self.index_manager.batch_update_index(alias, pairs)

        # 5) Return all inserted IDs
        return doc_ids

  
    def search(self, query):
        """Perform indexed search before full scan."""
        # print("query: ", query)

        # Handle exact match queries using Hash Index
        if isinstance(query, tuple):  # (key, value) for exact match
            key, value = query
            result = self.index_manager.search_hash(key, value)

            if result and all(isinstance(doc_id, int) for doc_id in result):
                return self.table(self.default_table_name).get(doc_ids=result)  # ✅ Fix applied

        # Handle range queries using B+ Tree
        elif isinstance(query, dict):  # {'age': (min, max)}
            key, (min_v, max_v) = list(query.items())[0]

            doc_ids = self.index_manager.search_btree_range(key, min_v, max_v)

            if doc_ids and all(isinstance(doc_id, int) for doc_id in doc_ids):
                return self.get(doc_ids=doc_ids)  # ✅ Fix applied


        # If it's a TinyDB query object, fallback to normal search
        else:
            return self.table(self.default_table_name).search(query)


        return []  # Return an empty list if query type is unsupported