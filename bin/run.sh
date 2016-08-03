#!/usr/bin/env bash

# Use this for testing the Spark Merge process. Allows switching between 'local' & 'cluster' mode.

hadoop fs -rm -r /ajay/output/out1
hadoop fs -rm -r /ajay/output/out2

SECONDS=0

#./localMerge.sh \
./sparkMerge.sh \
  -e /ajay/input/ajay1,/ajay/input/ajay3 \
  -i /ajay/input/ajay2,/ajay/input/ajay4 \
  -o /ajay/output/out1,/ajay/output/out2 \
  -p 2 \
  -k empno

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."