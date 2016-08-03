#!/usr/bin/env bash

# Use this for running in the standalone mode.

spark-submit \
  --master local[4] \
  --class io.infoworks.MergeDriver \
  --num-executors 1 \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  infoworks-spark-utils-1.0-SNAPSHOT.jar \
  $@
