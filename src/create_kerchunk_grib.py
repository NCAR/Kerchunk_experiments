import sys
import os
import ujson
import pdb

import cfgrib
import kerchunk.grib2
from pathlib import Path
from fsspec.implementations.local import LocalFileSystem
from kerchunk.combine import MultiZarrToZarr


PARAM_ID_STR = 'paramId'

def parse_grib(_file):
    """Read grib file and return filterBy paramId.
    This is used in filtering when reading with kerchunk-->scan_grib"""
    grb = cfgrib.open_file(_file)
    variables = grb.variables
    param_Ids = {}
    for var in variables:
        if len(var.dimensions) == 0: # skip dimension variable
            continue
        curId = var.data.index.filter_by_keys[PARAM_ID_STR]

def gen_json(file_url, write_json=False):
    print(f'generating {file_url}')
    #out = kerchunk.grib2.scan_grib(file_url, filter={'typeOfLevel':'surface', 'paramId': 31})
    pdb.set_trace()
    out = kerchunk.grib2.scan_grib(file_url, filter={'paramId': 31})
    outfile = f'{file_url}.json'
    if write_json is True:
        with fs.open(outfile, 'wb') as f:
            print(f'writing {outfile}')
            f.write(ujson.dumps(out).encode());
    return out

fs = LocalFileSystem()
filelist = []
if len(sys.argv) > 2:
    path = sys.argv[1]
    glob = sys.argv[2]
    print(f'{path} {glob}')
    for path in Path(path).rglob(glob):
        filelist.append(str(path))
    print(filelist)

so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first') # args to fs.open()
# default_fill_cache=False avoids caching data in between file chunks to lowers memory usage.

singles = []
for file in filelist:
    singles.append(gen_json(file)[0])
pdb.set_trace()
mzz = MultiZarrToZarr(singles, concat_dims=['time'],coo_map={"time": "INDEX"})
out = mzz.translate()
print(out)
with fs.open('out.json', 'wb') as f:
    print("wrinting mzz")
    f.write(ujson.dumps(out).encode())
