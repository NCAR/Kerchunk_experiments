# Kerchunk_experiments


### Purpose
This repository will contain examples on how to create kerchunk files and aggregations. As well as how to read them locally and remotely.



### create_kerchunk.py

```
python create_kerchunk.py -h
usage: ./create_kerchunk.py [-h] --action <combine|sidecar> --directory <directory> [--output_location <directory>] [--filename <output filename>] [--extensions <extension> [<extension> ...]]
                            [--variables <variable names> [<variable names> ...]] [--dry_run] [--make_remote] [--regex <regular expression>]

Creates kerchunk sidecar files of an entire directory structure.

optional arguments:
  -h, --help            show this help message and exit
  --action <combine|sidecar>, -a <combine|sidecar>
                        Specify whether to create to combine references or create sidecar files.
  --directory <directory>, -d <directory>
                        Directory to scan and create kerchunk reference files.
  --output_location <directory>, -o <directory>
                        Directory to place output files
  --filename <output filename>, -f <output filename>
                        Filename for output json.
  --extensions <extension> [<extension> ...], -e <extension> [<extension> ...]
                        Only process files of this extension
  --variables <variable names> [<variable names> ...], -v <variable names> [<variable names> ...]
                        Only gather specific variables. Variable names are case sensitive. Use the special keyword 'ALL' to separate all into individual files.
  --dry_run, -dr        Do a dry run of processing
  --make_remote, -mr    Additionally make a remote accessible copy of json
  --regex <regular expression>, -r <regular expression>
                        Combine references that match
```
