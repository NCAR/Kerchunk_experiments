#!/usr/bin/env python
import sys
import argparse
import xarray
import pdb



def check_chunks(filename, var=None):
    """Prints the chunks of a given variable, or all if var is not specified"""
    ds = xarray.open_dataset(filename)
    for i in ds.variables:
        print(f'{i}: ')
        print(f'   shape:  {ds.variables[i].shape}')
        print(f'   chunks: {ds.variables[i].encoding["chunksizes"]}')

def print_chunk(ds, varname):
    print(ds[varname])
    return ds[varname]

def convert_chunk(infile, outfile, chunksize, varname):
    if type(varname) is list:
        varname = varname[0]
    if ',' in chunksize:
        chunksize = chunksize.split(',')
        chunksize = tuple([int(x) for x in chunksize])
    else:
        chunksize = int(chunksize)
    ds = xarray.open_dataset(infile)
    pdb.set_trace()
    new_ds = ds.chunk(chunksize)
    if varname:
        new_encoding = new_ds[varname].encoding
        if type(chunksize) is int:
            new_chunk = [chunksize]*len(new_ds[varname].shape)
            for i,j in enumerate(new_chunk):
                if j > new_ds[varname].shape[i]:
                    new_chunk[i] = new_ds[varname].shape[i]
            new_chunk = tuple(new_chunk)
        else:
            new_chunk = chunksizes
        new_encoding['chunksizes'] = new_chunk
        for i in ['szip', 'zstd', 'bzip2', 'blosc', 'preferred_chunks', 'coordinates']:
            if i in new_encoding:
                new_encoding.pop(i)
        print(f'writing {outfile} with chunksize {new_chunk}')
        new_ds.to_netcdf(outfile, encoding = {varname:new_encoding})

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f'Usage {sys.argv[0]} [filename] [output filename] [chunksize] [variable (Optional)]')
        exit(1)
    if sys.argv[1] == 'check':
        check_chunks(sys.argv[2])
    else:
        convert_chunk(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:])
