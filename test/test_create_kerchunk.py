#!/usr/bin/env python

import sys
import os
import pdb
import glob
import ujson
sys.path.append('../src')
import create_kerchunk
from kerchunk.combine import MultiZarrToZarr


def test_find_files():
    regex = r'^.*19:.*$'
    extensions = ['.nc']
    path = '/gpfs/csfs1/collections/rda/data/d559000/wy1981/198102/'
    return create_kerchunk.find_files(path, regex, extensions)

def test_combine():
    create_kerchunk.process_kerchunk_combine(directory='/gpfs/csfs1/collections/rda/data/d559000/wy1981/198101/', output_directory='.', extensions=[], regex=r"^.*wrf2d.*-01.*$", output_filename="T2_P_198101.json", variables=['T2','P'], dry_run=False)

def test_create_mzz():
    path = '/gpfs/csfs1/collections/rda/data/d559000/wy1981/198102/wrf3d_d01_1981-02-27_0*'
    path = '/gpfs/csfs1/collections/rda/data/d633000/e5.oper.an.pl/194007/*_t*.nc'
    pdb.set_trace()
    files = glob.glob(path)
    print(files)
    all_refs = []
    for file in files:
        ref = create_kerchunk.gen_json(file)
        all_refs.append(ref)

    mzz = MultiZarrToZarr(
       all_refs,
       #concat_dims=["Time"],
       concat_dims=["time"],
       #coo_map='QSNOW',
    )
    multi_kerchunk = mzz.translate()

    # Write kerchunk .json record
    output_fname = "combined_kerchunk.json"
    with open(f"{output_fname}", "wb") as f:
        f.write(ujson.dumps(multi_kerchunk).encode())

def test_chunks():
    filename = '/gpfs/csfs1/collections/rda/data/d559000/wy1981/198101/wrf2d_d01_1981-01-31_19:00:00.nc'

#test_create_mzz()
#files = test_find_files()
#print(files)
test_combine()
