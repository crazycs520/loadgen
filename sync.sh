#!/bin/sh
while [ "true" ]; do
    echo '----------------------------------------------------------------------------'
    fswatch -r -L -1 *
    date
    rsync -Pav -e "ssh -i ~/.ssh/idc.pem" *  pingcap@172.16.5.40:/home/pingcap/cs/testutil
done