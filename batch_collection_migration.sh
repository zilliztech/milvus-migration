#!/bin/bash

collections=("prod_openai_biz_vector" "prod_openai_biz_knowledge" "prod_openai_biz_knowledge_table")

for collection in "${collections[@]}"; do
    echo "BatchMigration==> $collection"
#    /Users/zilliz/gitCode/cloud_team/milvus-migration/milvus-migration start --table="$collection"  --config=/Users/zilliz/gitCode/cloud_team/milvus-migration/configs/milvus_iterator/milvus2x_dynamic_field_cannotGetFieldname.yml
    /root/milvus-migration start -t="$collection"  -c=/root/milvus-migration/user_0827.yml
done

# chmod +x batch_collection_migration.sh