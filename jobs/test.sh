#!/bin/sh

echo "start job $* in `pwd`"
echo "some data" > data.txt
sleep 3
echo "end job $*"

echo "data.txt" > return_files.txt
