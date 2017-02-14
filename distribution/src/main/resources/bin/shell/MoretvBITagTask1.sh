#!/bin/bash
Args=$@
ShellHome=/app/bi/medusa/bin/shell
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.AddTagAndCommentOperation" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.AddTagAndCommentAccountIdOperation" $Args

$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.AddTagTotalAccount" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.AddTagTotalUsers" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.CommentTotalAccount" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.CommentTotalUsers" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.CollectSubTagOperation" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.CollecttagOperation" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.MoreTagOperation" $Args
$ShellHome/MoretvBIRun.sh "com.moretv.bi.tag.TagPagePVUVVV" $Args

