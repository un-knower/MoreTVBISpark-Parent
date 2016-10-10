#!/bin/bash
if [ ! -d target ];then
        mkdir -p target
fi
tar -zcvf ./target/MoreTVBISpark-1.0.0-SNAPSHOT-bin.tar.gz --exclude=target ./*