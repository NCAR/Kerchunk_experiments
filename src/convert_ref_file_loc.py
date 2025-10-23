#!/usr/bin/env python
import sys
# import os
# import re
# import pdb


def main(filename, outfile):
    """ Convert local file path to remote https file path
    There are two remote paths:
    1. https://data.gdex.ucar.edu
    2. https://osdf-director.osg-htc.org/ncar/gdex

    The remote referece files will be saved as:
    1. [outfile]-https.json
    2. [outfile]-osdf.json
    respectively.

    The 

    Parameters
    ----------
    filename : str
        Input local reference file name
    outfile : str
        Output remote reference file name. 

    """
    out_filename = outfile.split('.json')[0]+'-https.json'
    ofile = open(out_filename, 'w', encoding='utf-8')
    osdf_filename = outfile.split('.json')[0]+'-osdf.json'
    osdf_ofile = open(osdf_filename, 'w', encoding='utf-8')
    with open(filename, 'r', encoding='utf-8') as fh:
        for l in fh:
            # Find local file path pattern and replace with remote https path
            # Note: change to new GDEX path
            match = '\\/glade\\/campaign\\/collections\\/gdex\\/data'
            replacement = 'https:\\/\\/data.gdex.ucar.edu'
            replacement_osdf = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/gdex'
            # Note : old RDA path
            # match = '\\/gpfs\\/csfs1\\/collections\\/rda\\/data'
            # replacement = 'https:\\/\\/data.rda.ucar.edu'
            # replacement_osdf = 'https:\\/\\/osdf-director.osg-htc.org\\/ncar\\/rda'

            # replace local reference to remote reference  
            str_output =  l.replace(match, replacement)
            str_output_osdf =  l.replace(match, replacement_osdf)

            # output to remote reference file
            ofile.write(str_output)
            osdf_ofile.write(str_output_osdf)
            #print(l)

    print(f'Created: {out_filename}')
    print(f'Created: {osdf_filename}')

def main_parquet(filename, outfile):
    """ Convert local file path to remote https file path in parquet files
    There are two remote paths:
    1. https://data.gdex.ucar.edu
    2. https://osdf-director.osg-htc.org/ncar/gdex

    The remote reference files will be saved as:
    1. [outfile]-https.parquet
    2. [outfile]-osdf.parquet
    respectively.

    Parameters
    ----------
    filename : str
        Input local parquet reference file name
    outfile : str
        Output remote parquet reference file name. 

    """
    import pandas as pd

    # Read the parquet file
    df = pd.read_parquet(filename)

    # Define path patterns for replacement
    match = '/glade/campaign/collections/gdex/data'
    replacement = 'https://data.gdex.ucar.edu'
    replacement_osdf = 'https://osdf-director.osg-htc.org/ncar/gdex'

    # Create copies of the dataframe for modifications
    df_https = df.copy()
    df_osdf = df.copy()

    # Replace paths in all string columns
    #  Parquet files have 4 Columns: ['path', 'offset', 'size', 'raw']
    #  We modify the 'path' column which is of string type

    # check path column exist
    if 'path' not in df.columns:
        print(f'Column "path" not found in the parquet file.')
        sys.exit(1)
    if df['path'].dtype != 'object':  # String column
        print(f'Column "path" is not of string type in the parquet file.')
        sys.exit(1)

    # replace local reference to remote reference
    column = 'path'
    df_https[column] = df_https[column].astype(str).str.replace(match, replacement, regex=False)
    df_osdf[column] = df_osdf[column].astype(str).str.replace(match, replacement_osdf, regex=False)

    # Generate output filenames
    base_name = outfile.split('.parq')[0] if outfile.endswith('.parq') else outfile
    out_filename_https = base_name + '-https.parq'
    out_filename_osdf = base_name + '-osdf.parq'

    # Write the modified dataframes to parquet files
    df_https.to_parquet(out_filename_https, index=False)
    df_osdf.to_parquet(out_filename_osdf, index=False)

    print(f'Created: {out_filename_https}')
    print(f'Created: {out_filename_osdf}')


if __name__ == "__main__":
    # test if arguments are provided
    if len(sys.argv) < 3:
        print('Convert local file to remote file location')
        print('For JSON files:')
        print(f'  usage: {sys.argv[0]} [filename.json] [outfile.json]')
        print('For Parquet files:')
        print(f'  usage: {sys.argv[0]} [filename.parquet] [outfile.parquet]')
        sys.exit(1)

    filename = sys.argv[1]
    outfile = sys.argv[2]

    # Determine file type and call appropriate function
    if filename.lower().endswith('.parquet'):
        main_parquet(filename, outfile)
    else:
        # Default to JSON processing for backwards compatibility
        main(filename, outfile)
