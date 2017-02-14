#!/bin/bash

Offset=1
Args="--offset $Offset"
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.login.WeeklyActiveUser" $Args
