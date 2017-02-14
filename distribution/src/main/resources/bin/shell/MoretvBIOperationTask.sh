#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.LiveOKButtonUsage" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.MovieUpDown" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.ProgramEvaluateList" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.SubjectCollectQuantity" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.SubjectCollectUsage" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.operation.NotrecommendPVUV" $Args
