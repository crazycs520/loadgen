#!/bin/bash

set -ex

./deploy.sh db1 v8.5.3 topology.yaml
./sysbench.sh 127.0.0.1 4000 prepare.sql logs
./tpcc.sh 127.0.0.1 4000 prepare.sql logs

tiup cluster stop db1 -y
