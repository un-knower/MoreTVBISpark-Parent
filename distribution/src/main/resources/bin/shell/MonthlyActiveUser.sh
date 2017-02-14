#!/bin/bash

Offset=5
Args="--offset $Offset"
/app/bi/medusa/bin/shell/MoretvBIRun.sh "com.moretv.bi.login.MonthlyActiveUser" $Args
