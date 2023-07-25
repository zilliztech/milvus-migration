# Milvus Migration: Milvus1.x  to Milvus 2.x

## Limitation

### Soft version
- Source Milvus version:  0.9.x ~ 1.x
- Target Milvus version:  2.2+

## Milvus 0.9.x ~ 1.x(sqlite) to Milvus 2.x migration.yaml example
use sqlite storage Milvus 0.9.x ~ 1.x collection meta data.
```yaml
dumper:
  worker:
    limit: 20
    workMode: milvus1x
    reader:
      bufferSize: 1024 # unit is KB
    writer:
      bufferSize: 1024 # unit is KB

loader:
  worker:
    limit: 20

meta:
  mode: sqlite    # mode set sqlite will get meta info from sqlite
  sqliteFile: /db/meta.sqlite

source:
  mode: local   # source mode can set local or remote(s3,minio,gcp,ali-oss)
  local:
    tablesDir: /db/tables/

target:
  mode: remote  
  remote:
    outputDir: output/ # don't start with /

    cloud: aws
    endpoint: 127.0.0.1:9000
    region: ap-southeast-1
    bucket: a-bucket
    ak: minioadmin
    sk: minioadmin
    useIAM: false
    useSSL: false
    checkBucket: true

  milvus2x: # milvus2x connect info
    endpoint: xxxxxx:19530
    username: xxxxx
    password: xxxxx
```
if `source.mode` is `remote`, your source config maybe like below:
```yaml
...
source:
  mode: remote
  remote:
    cloud: aws
    region: us-west-2
    bucket: xxxxx
    ak: xxx
    sk: xxx
    useIAM: false
    tablesDir: /xxx/tables/
...
```


### Migrate milvus 0.9.x ~ 1.x(mysql) to Milvus 2.x

```yaml
dumper:
  worker:
    limit: 20 # dumper thread concurrency
    workMode: milvus1x
    reader:
      bufferSize: 1024 # unit is KB
    writer:
      bufferSize: 1024 # unit is KB

loader:
  worker:
    limit: 20 # loader thread concurrency

meta:
  mode: mysql
  mysqlUrl: "user:password@tcp(localhost:3306)/milvus?charset=utf8mb4&parseTime=True&loc=Local"


source:
  mode: local
  local:
    tablesDir: /db/tables/

target:
  mode: remote
  remote:
    outputDir: output/ # don't start with /
    cloud: aws
    endpoint: 127.0.0.1:9000
    region: ap-southeast-1
    bucket: a-bucket
    ak: minioadmin
    sk: minioadmin
    useIAM: false
    useSSL: false
    checkBucket: true

  milvus2x: # milvus2x connect info
    endpoint: xxxxxx:19530
    username: xxxxx
    password: xxxxx
```
Choice your migration.yaml and place to configs/ directory, then tool will auto read config from the configs/migration.yaml
migration Milvux1.x-2.x need execute `dump` cmd first, when `dump` finished execute `load` cmd to finish migration.
execute `dump` cmd: will dump the source data to numpy:  
```shell
./milvus-migration  dump
#or you can place the migration.yaml to any directory, then set`--config` param to the path:
./milvus-migration  dump --config=/{YourConfigFilePath}/migration.yaml
```
execute `load` cmd: will load the numpy files to Milvus 2.x:
```shell
./milvus-migration  load
#or you can place the migration.yaml to any directory, then set`--config` param to the path:
./milvus-migration  load --config=/{YourConfigFilePath}/migration.yaml
```
finally migration success, and you will see the print log like below:
```html
["[Loader] migration 1.x to Milvus finish!!!"] [Cost=80.009174459]
["[Loader] Load Success!"] [Cost=80.00928425]
[cleaner/remote_cleaner.go:27] ["[Remote Cleaner] Begin to clean files"] [bucket=a-bucket] [rootPath=testfiles/output/zwh/migration]
["[Cleaner] clean file success!"]
```
if you want to verify the migration data result, you can use Attu see your new collection info. [Attu](https://github.com/zilliztech/attu)


## migration.yaml reference

### `dumper`

| Parameter                       | Description                                         | Example                                                         |
|---------------------------------|-----------------------------------------------------|-----------------------------------------------------------------|
| dumper.worker.workMode          | Work mode for milvus-migration dumper               | milvus1x: dump data from Milvus1.x; faiss: dump data from Faiss |
| dumper.worker.limit             | The number of dumper threads to run concurrently    | 20: means to dump 20 segment files simultaneously               |
| dumper.worker.reader.bufferSize | The buffer size for each segment file reader, in KB | 1024                                                            |
| dumper.worker.writer.bufferSize | The buffer size for each segment file writer, in KB | 1024                                                            |

### `loader`

| Parameter           | Description                   | Example                                           |
|---------------------|-------------------------------|---------------------------------------------------|
| loader.worker.limit | Concurrency of loader threads | 20: means load 20 segments files at the same time |

### `meta`

| parameter       | description                                                           | example                                                                             |
|-----------------|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| meta.mode       | Where to read the source meta information from                        | mock/mysql/sqlite/remote                                                            |
| meta.mockFile   | When meta.mode is mock, read milvus1.x meta info from local meta.json |                                                                                     |
| meta.sqliteFile | When meta.mode is mysql, read milvus1.x meta info from meta.sqlite    | /home/root/milvus/db/meta.sqlite                                                    |
| meta.mysqlUrl   | When meta.mode is sqlite, read milvus1.x meta info from mysql         | "user:password@tcp(localhost:3306)/milvus?charset=utf8mb4&parseTime=True&loc=Local" |

### `source`

| parameter              | description                                       | example                                                       |
|------------------------|---------------------------------------------------|---------------------------------------------------------------|
| source.mode            | Where the source files are read from              | local: read files from local disk, remote: read files from S3 |
| source.local.tablesDir | Position of the Milvus 0.9.x~1.x tables directory | /home/${user}/milvus/db/tables                                |

### `target`

| parameter                           | description                                          | example                                                                   |
|-------------------------------------|------------------------------------------------------|---------------------------------------------------------------------------|
| target.mode                         | Where to store the dumped files                      | local: store dumped files on local disk; remote: store dumped files on S3 |
| target.remote.outputDir             | Directory path in bucket where to store files        | output/                                                                   |
| target.remote.cloud                 | Storage in Milvus 2.x                                | aws (if using Minio, use aws), GCP, or Azure                              |
| target.remote.endpoint              | Endpoint of the Milvus 2.x storage                   | 127.0.0.1:9000                                                            |
| target.remote.region                | Region of the Milvus 2.x storage                     | If using local Minio, can use any value                                   |
| target.remote.bucket                | Bucket of the Milvus 2.x storage                     | Must use the same bucket as configured in milvus.yaml for Milvus 2.x      |
| target.remote.ak                    | Access Key of the Milvus 2.x storage                 | minioadmin                                                                |
| target.remote.sk                    | Secret Key of the Milvus 2.x storage                 | minioadmin                                                                |
| target.remote.useIAM                | Whether to use IAM Role to connect to Milvus 2.x     | false                                                                     |
| target.remote.useSSL                | Whether to use SSL when connecting to Milvus 2.x     | For local Minio, use false; for remote S3, use true                       |
| target.remote.checkBucket           | Whether to check if the bucket exists in the storage | True to check if you can connect to the Milvus 2.x storage                |
| target.milvus2x.endpoint            | Endpoint of Milvus 2.x                               | xxxxxx:19530                                                              |
| target.milvus2x.username            | Username of Milvus 2.x                               | root                                                                      |
| target.milvus2x.password            | Password of Milvus 2.x                               | xxxxxxx                                                                   |
