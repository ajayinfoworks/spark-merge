#!/usr/bin/env bash

# Use this for running in the cluster mode.

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class io.infoworks.MergeDriver \
  --num-executors 1 \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  infoworks-spark-utils-1.0-SNAPSHOT.jar \
  $@
