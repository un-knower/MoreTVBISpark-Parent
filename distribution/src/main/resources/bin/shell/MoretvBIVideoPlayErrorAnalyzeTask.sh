#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.VideoPlayErrorAnalyze.play_isorno" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.VideoPlayErrorAnalyze.PlaySourceErrorStatisitcs" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.VideoPlayErrorAnalyze.ProgramPlayErrorStatistics" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.VideoPlayErrorAnalyze.SourceStatistics" $Args




