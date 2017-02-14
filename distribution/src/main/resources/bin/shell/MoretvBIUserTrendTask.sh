#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.UserTrend.User_use_duration" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.UserUseDurationAndTimes.User_shichang" $Args



