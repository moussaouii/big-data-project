#!/bin/bash


spark-submit \
app.py \
--master yarn \
--deploy-mode cluster \
--driver-memory 4g --executor-memory 2g --executor-cores 1







