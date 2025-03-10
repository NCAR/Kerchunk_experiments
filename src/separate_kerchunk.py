import sys
import os
import json
import xarray
import dask

def separate(filename):
    refs = json.load(open(filename))
    refs = refs['refs']
    var_names = list(refs.keys())
    unique_vars = set()
    for v in var_names:
        unique_vars.add(v.split('/')[0]
    var_types = get_var_types(filename)


def get_var_types(filename):
    """Return variable names that are primary or coordinates.
    """
    ds = open_local_kerchunk(filename)

def open_local_kerchunk(filename):
    fs = fsspec.filesystem('reference', fo=filename)
    m = fs.get_mapper('')
    ds = xarray.open_dataset(m, engine='zarr', backend_kwargs={'consolidated':False})
    return ds

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(f'Usage: {sys.argv[0]} [filename]')
    filename = sys.argv[1]
    separate(fileanme)

