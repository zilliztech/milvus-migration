from pymilvus import Collection
from pymilvus import connections


connections.connect(host="localhost", port="19530")
collection_name = "test1w"
collection_size = 10000
collection = Collection(collection_name)
# 1. check the collection size migrated from milvus 1.x
print(collection.num_entities)
assert collection.num_entities == collection_size

# 2. create index
# index_param = {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 48, "efConstruction": 500}}
index_param = {"index_type": "FLAT", "metric_type": "L2", "params": {}}
collection.create_index("data", index_param, index_name="index_name_1")
collection.load()

# 3. check data not change after migration in milvus2x
exp = "id>0"
query_results = collection.query(exp, output_fields=['id', 'data'])
id = 1
dim = 256
for query_result in query_results:
    assert query_result['id'] == id
    assert query_result['data'] == [1.0 for i in range(dim)]
    id += 1

# 4. search
top_k = 100
# default_search_params = {"metric_type": "L2", "params": {"ef": 500}}
default_search_params = {"metric_type": "L2", "params": {}}
search_result = collection.search([query_results[0]['data']], anns_field="data", param=default_search_params, limit=top_k)
print("Search data is: \n")
print(query_results[0]['data'])
print("Search data id is: \n")
print(query_results[0]['id'])
print("Searched result is (id): \n")
print(search_result[0].ids)
assert len(search_result[0].ids) == top_k
# assert search_result[0].ids[0] == query_results[0]['id']



