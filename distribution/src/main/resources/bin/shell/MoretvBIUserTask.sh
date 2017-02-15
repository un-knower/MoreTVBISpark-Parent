#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.UserAgeStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.UserGenderStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.UserGeographyStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.user.YunOSNewUser" $Args
