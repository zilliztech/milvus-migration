from elasticsearch import Elasticsearch

# 1. connect to es
client = Elasticsearch("http://localhost:9200")
res = client.info()
print(res)

# 2. create index
client.indices.create(index="test_es_index")
dim = 128
vector_single = [1.0 for _ in range(dim)]

# 3. insert data
number = 10000
for i in range(number):
    doc = {'int64': i, 'string': str(i), 'vector': vector_single}
    res = client.index(index="test_es_index", id=i, document=doc)
    print(res['result'])

# 4. check data
res = client.get(index="test_es_index", id=0)
print(res['_source'])
assert res['_source']["int64"] == 0
assert res['_source']["string"] == '0'
assert res['_source']["vector"] == vector_single
