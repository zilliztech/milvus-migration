#!/bin/bash

collections=("collection1" "collection2" "collection3")

for collection in "${collections[@]}"; do
    echo "BatchMigration==> $collection"
    ./milvus-migration start -t="$collection" -c=/{YourConfigPath}/migration.yml
done

# how to execute?
#1. chmod +x batch_collection_migration.sh
#2. ./batch_collection_migration.sh