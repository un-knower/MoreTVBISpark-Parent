#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.history.History_PVUVVV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.history.CollectStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.history.HistoryAndCollectPageStatistics" $Args
