# Milvus Migration: Milvus2.x to Milvus2.x

## Limitation

### Soft version

- Source Milvus support version:  2.3.0+
- Target Milvus support version: 2.2+


## Milvus2.x to Milvus2.x migration.yaml example

```yaml
dumper:
  worker:
    workMode: milvus2x      # work mode：milvus2x->milvus2x
    reader:
      bufferSize: 500       # Read source data rows in each time read from Source Milvus.

meta:                       # meta part
  mode: config              # 'config' mode means will get meta config from this config file itself.
  version: 2.3.0            #  Source Milvus version
  collection: src_coll_name # migrate data from this source collection

source:                     # source milvus connection info
  milvus2x:
    endpoint: {milvus2x_domain}:{milvus2x_port}
    username: xxxx
    password: xxxxx

target:                    # target milvus collection info
  milvus2x:
    endpoint: {milvus2x_domain}:{milvus2x_port}
    username: xxxx
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
[INFO] [migration/milvus2x_starter.go:79] ["=================>JobProcess!"] [Percent=100]
[INFO] [migration/milvus2x_starter.go:27] ["[Starter] migration Milvus2X to Milvus2X finish!!!"] [Cost=94.877717375]
[INFO] [starter/starter.go:109] ["[Starter] Migration Success!"] [Cost=94.878243583]
[INFO] [cleaner/none_cleaner.go:17] ["[None Cleaner] not need clean files"] [mode=]
[INFO] [cmd/start.go:32] ["[Cleaner] clean file success!"]
```
if you want to verify the migration data result, you can use Attu see source collection already in your target Milvus. [Attu](https://github.com/zilliztech/attu)  

## Other introduce
- if you don't migration all the source collection fields to the target Milvus, you can add fields config in meta part to specify the need migration fields.
- btw, you at least need migration the PrimaryKey and Vector type field to target Milvus.
```yaml
...
meta:
  #......
  fields:          # optional configuration, only migration below source collection fields to target milvus:
    - name: id
    - name: title_vector
    - name: reading_time
  #......
...
```
- if you want to customize target collection properties, you can add below config in your meta part
```yaml
...
meta:
  #......
  milvus:               #below info are target collection optional configuration:
    collection: target_coll_name  # If not, the source collection name will be used.
    closeDynamicField: false      # If not, the source collection DynamicField prop will be used.
    shardNum: 2                   # If not, the source collection ShardNum prop will be used.
    consistencyLevel: Customized  # If not, the source collection consistencyLevel prop will be used.
  #......  
...
```
- if you want to customize source or target milvus grpc connection request or receive message max size, you can add below config like:
```yaml
...
  source:                     
    milvus2x:
      ...
      grpc:
        maxCallRecvMsgSize: 67108864
        maxCallSendMsgSize: 268435456
      ...

  target:                    
    milvus2x:
      ...
      grpc:
        maxCallRecvMsgSize: 67108864
        maxCallSendMsgSize: 268435456
      ...
...