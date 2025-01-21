#!/bin/bash 

while read -r line; do
    regex=`echo $line | awk '{printf $1}'`
    filename=`echo $line | awk '{printf $2}'`
    echo $regex;
    echo $filename
    ./create_kerchunk.py -d /gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.fc.sfc.instan/ -a combine --regex "^.*e5.oper.fc.sfc.instan.*$regex.*.nc$" -f "$filename" -mr -o instan633.0
done < instan.txt

