#!/usr/bin/env python
import sys
import os
import re
import pdb



def main(filename, outfile):
    ofile = open(outfile, 'w')
    osdf_filename = outfile.split('.json')[0]+'osdf.json'
    osdf_ofile = open(osdf_filename, 'w')
    with open(filename) as fh:
        for l in fh:
            match = '\\/gpfs\\/csfs1\\/collections\\/rda\\/data'
            replacement = 'https:\\/\\/data.rda.ucar.edu'
            replacement_osdf = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/rda'
            str_output =  l.replace(match, replacement)
            str_output_osdf =  l.replace(match, replacement_osdf)
            ofile.write(str_output)
            osdf_ofile.write(str_output_osdf)
            #print(l)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f'Convert local file to remote file location')
        print(f'usage: {sys.argv[0]} [filename] [outfile]')
        exit(1)
    main(sys.argv[1], sys.argv[2])
