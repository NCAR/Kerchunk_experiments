#!/usr/bin/env python
import sys
import os
import re
import pdb



def main(filename, outfile):
    ofile = open(outfile, 'w')
    with open(filename) as fh:
        for l in fh:
            match = '\\/gpfs\\/csfs1\\/collections\\/rda\\/data'
                    #\\/gpfs\\/csfs1\\/collections\\/rda\\/data
            #replacement = 'https:\\/\\/data.rda.ucar.edu'
            replacement = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/rda'
            str_output =  l.replace(match, replacement)
            ofile.write(str_output)
            print(l)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f'Convert local file to remote file location')
        print(f'usage: {sys.argv[0]} [filename] [outfile]')
        exit(1)
    main(sys.argv[1], sys.argv[2])
