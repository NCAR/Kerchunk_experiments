#!/bin/bash 

while read -r line; do
    regex=`echo $line | awk '{printf $1}'`
    filename=`echo $line | awk '{printf $2}'`
    echo $regex;
    echo $filename
    ./create_kerchunk.py -d /gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.fc.sfc.accumu/ -a combine --regex "^.*e5.oper.fc.sfc.accumu.*$regex.*.nc$" -f "$filename" -mr -o accum633.0
done < accum.txt

