#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay
./execute.pl -s  -L total.log -l partial.log -n RIOTester -f 0 -c scripts/RIOTest

