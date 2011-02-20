#!/bin/bash

rm -rf storage
rm -f *.log
rm -f *.replay
./execute.pl -s -n DistNode -f 4 -c scripts/DistNode

