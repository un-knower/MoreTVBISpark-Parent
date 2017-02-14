#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.content.SubjectChannelPVUV" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.content.SubjectChannelClassifiedStatistics" $Args
