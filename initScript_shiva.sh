#!/bin/bash
export BIGFUN_HOME=$HOME/bigFUN;
cd $BIGFUN_HOME;
mvn clean package;
$BIGFUN_HOME/scripts/run-bigfun.sh
python plot.py

