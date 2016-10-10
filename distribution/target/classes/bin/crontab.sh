#!/bin/sh
day=`date -d 'today' +%Y%m%d`
lastDay=`date -d 'yesterday' +%Y%m%d`

dir='/script/bi/medusa/MoreTVBISpark-1.0.0-SNAPSHOT-bin/bin'
Args="--startDate $day --deleteOld true"

###################Processing the task lists###################
