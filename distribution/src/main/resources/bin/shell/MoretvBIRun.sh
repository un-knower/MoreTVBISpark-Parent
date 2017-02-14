#!/bin/bash
Params=($@)
MainClass=${Params[0]}
Length=${#Params[@]}
Args=${Params[@]:1:Length-1}

source ~/.bash_profile
/app/bi/medusa/bin/shell/submit.sh $MainClass $Args
