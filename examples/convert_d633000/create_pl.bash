#!/bin/bash 

for decade in {4..9}; do
while read -r line; do
    regex=`echo $line | awk '{printf $1}'`
    filename=`echo $line | awk '{printf $2}'`
    echo $regex;
    echo $filename
    ./create_kerchunk.py -d /gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.an.pl/ -a combine --regex "^.*/19$decade.../.*e5.oper.an.pl.*$regex.*.nc$" -f "${filename}.19${decade}0-19${decade}9" -mr -o pl633.0 
done < an.pl.txt
done

for decade in {0..2}; do
while read -r line; do
    regex=`echo $line | awk '{printf $1}'`
    filename=`echo $line | awk '{printf $2}'`
    echo $regex;
    echo $filename
    ./create_kerchunk.py -d /gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.an.pl/ -a combine --regex "^.*/20$decade.../.*e5.oper.an.pl.*$regex.*.nc$" -f "${filename}.20${decade}0-20${decade}9" -mr -o pl633.0 
done < an.pl.txt
done

