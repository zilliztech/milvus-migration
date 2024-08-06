# Milvus Migration: Elasticsearch to Milvus 2.x

## Limitation

### Soft version

- Elasticsearch support version:  7.x or 8.x
- Milvus2.x support version: 2.2+

### Support ES field type

Tool will migration ES index data to Milvus2.x Collection. Now support migration es field type have below:

| ES field Type | Mapping field type of Milvus2.x collection |
|:--------------|:-------------------------------------------|
| keyword       | VarChar                                    |
| text          | VarChar                                    |
| long          | int64                                      |
| integer       | int32                                      |
| short         | int16                                      |
| double        | Double                                     |
| float         | Float                                      |
| boolean       | Bool                                       |
| dense_vector  | FloatVector                                |

### other limitation

1. must need migrate a `dense_vector` type es field to Milvus2.x as collection's FloatVector field.
2. must need migrate a `keyword` or `long` type es field as Collection primary key, if not specified will use `_id` as
   primary key.

## Elasticsearch to Milvus 2.x migration.yaml example

```yaml
dumper:
  worker:
    workMode: elasticsearch   # migration mode: elasticsearch, faiss, milvus1x
    reader:
      bufferSize: 2500      # es data total rows in each time read from es.  
meta:
  mode: config            #config: will read meta config from this config itself.
  index: test_es_index    #es index 
  fields:                 # es index fields of need migrate to milvus2x 
    - name: id            #field name
      type: long
      pk: true            #set pk=true will use this field as milvus collection primary key 
    - name: data
      type: dense_vector  #dense_vector field have to migrate field.
      dims: 512           #vector dims
    - name: other_field
      type: keyword
      maxLen: 60          #as milvus collection Varchar type filed maxLen property.
  milvus:               #optional configuration
    collection: rename_test_name  #if not exits, will use es index as collection name. 
    closeDynamicField: false
    shardNum: 2
    consistencyLevel: Customized
source:               # es connect configuration
  es:
    urls:
      - http://localhost:9200
    username: x*****x
    password: x*****x
target:
  mode: remote
  remote:
    outputDir: migration/test/xx   # temp storage source data, cannot start with '/'
    cloud: aws                    #cloud includes: aws(s3,minio), gcp, ali(oss)
    endpoint: localhost:9000     # here is the minio server endpoint 
    region: ap-southeast-1       # region, if local server needn't it
    bucket: a-bucket      # bucket name, need same with milvus2.x bucket name
    ak: minioadmin        # ak/sk, if useIAM=false, need ak/sk
    sk: minioadmin
    useIAM: false         #if useIAM=true, not need ak/sk. 
    useSSL: false
    checkBucket: true     #if bucket not exits will create bucket when set checkBucket=true  
    
  milvus2x:
    endpoint: localhost:19530
    username: xxxxx
    password: xxxxx
```

you can place the migration.yaml to configs/ directory, then tool will auto read config from the configs/migration.yaml
when you execute cmd:

```shell
./milvus-migration  start
```

or you can place the migration.yaml to any directory, then will read config from `--config` param path when execute cmd
like below:

```shell
./milvus-migration  start --config=/{YourConfigFilePath}/migration.yaml
```
migration success when you see the print log like below:
```html
[migration/es_starter.go:25] ["[Starter] migration ES to Milvus finish!!!"] [Cost=80.009174459]
[starter/starter.go:106] ["[Starter] Migration Success!"] [Cost=80.00928425]
[cleaner/remote_cleaner.go:27] ["[Remote Cleaner] Begin to clean files"] [bucket=a-bucket] [rootPath=testfiles/output/zwh/migration]
[cmd/start.go:32] ["[Cleaner] clean file success!"]
```
if you want to verify the migration data result, you can use Attu see your new collection info. [Attu](https://github.com/zilliztech/attu)  

## Other introduce
- if you don't config pk=true field, default will use es `_id` as primary key, pk type is Varchar and maxLen is 65535. 
  Usually, this is not a good pk strategy, so here provide to explicit setting like below config:
```yaml
...
meta:
  fields:
    - name: _id      # explicit _id as primary key a field  
      type: keyword  # primary key type specified `keyword` type
      maxLen: 60     # primary key maxLen set 60
      pk: true
...
```
also you can change type to long
```yaml
...
meta:
  fields:
    - name: _id
      type: long  # primary key type specified `long` type
      pk: true
...
```
- if your es server using the Elastic Cloud es, then you can config like below to connect es: 
```yaml
...
source:  
   es:
      cloudId: x*********x
      apiKey:  x*******x
...
```
- if your es server setting others auth style, like: serviceToken, fingerprint, ca file, you can add corresponding authorization config:
```yaml
...
source:  
   es:
      fingerprint: x********x
      serviceToken:  x******x
      cert:  /{YourPath}/http_ca.cert
...
```
- About target, if you use aliyun-oss, your config will like below:
```yaml
target:
  mode: remote
  remote:
    outputDir: "migration/test/xxx"
    cloud: ali    # cloud set is ali
    endpoint: oss-{YourRegion}-internal.aliyuncs.com  # if dont set oss endpoint default is: oss.aliyuncs.com
    region: { yourRegion }
    bucket: { yourBucket }
    useIAM: true    #useIAM=true, don't need ak/sk
    checkBucket: false
    useSSL: true
```
or if you are use s3:
```yaml
target:
  mode: remote
  remote:
    outputDir: "migration/test/xxx"
    cloud: aws
    region: { yourRegion }
    bucket: { yourBucket }
    useIAM: true      #use IAM connect s3
    checkBucket: false
    useSSL: true
```

- If want to migrate data to Target Milvus collection (isn't `default` database), you can add `target.milvus2x.database` to specify database name, database name will auto create if not exists.
```yaml
...
    target:
      milvus2x:
        ...
        database: my_database
...
```

## migration.yaml reference

### `dumper`

Here dumper module mainly sets adjustment parameters for migration job.

| Parameter                       | Description                                    | Example            |
|---------------------------------|------------------------------------------------|--------------------|
| dumper.worker.workMode          | Work mode                                      | elasticsearch      |
| dumper.worker.reader.bufferSize | how many rows data read from es in every batch | suggest: 2000-4000 |

### `meta`

Here meta module mainly sets source data(es index info) and will create target data(milvus2.x collection info)

| parameter                     | description                                         | example                                            |
|-------------------------------|-----------------------------------------------------|----------------------------------------------------|
| meta.mode                     | Where to read meta config, now only support: config | config: represents read from migration.yaml itself |
| meta.index                    | Read data from which es index                       | test_es_index                                      |
| meta.fields                   | Which es index fields need to be migrated           | field info below:                                  |
| meta.fields.-name             | es field name                                       | id                                                 |
| meta.fields.-pk               | Whether es field as primary key                     | true, default: false                               |
| meta.fields.-type             | es field type                                       | long, integer,keyword,float,dense_vector...        |
| meta.fields.-maxLen           | keyword or text es field maxLen in 2.x collection   | 100, default: 65535                                |
| meta.fields.-dims             | dense_vector type field dimension                   | 512                                                |
| meta.milvus                   | not required, set create 2.x collection property    | below:                                             |
| meta.milvus.collection        | 2.x collection name                                 | if null will use es index name as collection name  |
| meta.milvus.closeDynamicField | whether close 2.x Collection dynamic field feature  | default: false                                     |
| meta.milvus.consistencyLevel  | 2.x Collection consistency level                    | default: collection default level                  |

### `source`

Here source module mainly sets source es connection info

| parameter              | description            | example               |
|------------------------|------------------------|-----------------------|
| source.es.urls         | es server address list | http://localhost:9200 |
| source.es.username     | es server username     | xxx                   |
| source.es.password     | es server password     | xxx                   |
| source.es.cert         | es  cert file path     | /xxx/http_ca.crt      |
| source.es.fingerprint  | es  fingerprint        | xxxxxxxxxxxx          |
| source.es.serviceToken | es server serviceToken | Bearer xxxxxxxxxx     |
| source.es.cloudId      | elasticCloud cloudId   | xx                    |
| source.es.apiKey       | elasticCloud apiKey    | xxx                   |

### `target`

Here target module mainly sets milvus2.x server info

| parameter                 | description                                          | example                                                                   |
|---------------------------|------------------------------------------------------|---------------------------------------------------------------------------|
| target.mode               | Where to store the dumped files                      | local: store dumped files on local disk; remote: store dumped files on S3 |
| target.remote.outputDir   | Directory path in bucket where to store files        | output/                                                                   |
| target.remote.cloud       | Which Cloud Storage the Milvus 2.x data              | aws (if using Minio, use aws), gcp, or azure, ali                         |
| target.remote.endpoint    | Endpoint of the Milvus 2.x storage                   | 127.0.0.1:9000                                                            |
| target.remote.region      | Region of the Milvus 2.x storage                     | If using local Minio, can use any value                                   |
| target.remote.bucket      | Bucket of the Milvus 2.x storage                     | Must use the same bucket as configured in milvus.yaml for Milvus 2.x      |
| target.remote.ak          | Access Key of the Milvus 2.x storage                 | minioadmin                                                                |
| target.remote.sk          | Secret Key of the Milvus 2.x storage                 | minioadmin                                                                |
| target.remote.useIAM      | Whether to use IAM Role to connect to Milvus 2.x     | false                                                                     |
| target.remote.useSSL      | Whether to use SSL when connecting to Milvus 2.x     | For local Minio, use false; for remote S3, use true                       |
| target.remote.checkBucket | Whether to check if the bucket exists in the storage | True to check if you can connect to the Milvus 2.x storage                |
| target.milvus2x.endpoint  | Endpoint of Milvus 2.x                               | xxxxxx:19530                                                              |
| target.milvus2x.username  | Username of Milvus 2.x                               | root                                                                      |
| target.milvus2x.password  | Password of Milvus 2.x                               | xxxxxxx                                                                   |

