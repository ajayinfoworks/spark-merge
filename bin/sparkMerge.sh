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
  --jars /usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar \
  infoworks-spark-utils-1.0-SNAPSHOT.jar \
  $@
