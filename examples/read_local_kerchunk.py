#!/usr/bin/env python
import fsspec
import sys
import xarray
import dask
import datetime


def read_local_ref(filename, *args):
    fs = fsspec.filesystem('reference', fo=filename)
    m = fs.get_mapper('')
    ds = xarray.open_dataset(m, engine='zarr', backend_kwargs={'consolidated':False})
    print(ds)
    if args:
        start = datetime.datetime.now()
        print(start)
        #ds.isel(south_north=500, west_east=500).load()
        end = datetime.datetime.now()
        print(end-start)

    return ds


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(f'Usage {sys.argv[0]} [filename]')
        exit(1)
    read_local_ref(sys.argv[1], sys.argv[2:])
