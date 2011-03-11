#!/bin/bash

javac -d bin -cp ./jars/plume.jar:./jars/lib.jar `find proj/ -name *.java`

exit
