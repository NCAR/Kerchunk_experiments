#!/usr/bin/env python

import sys
import os
import pdb
import glob
import ujson
sys.path.append('../src')
import create_kerchunk
from kerchunk.combine import MultiZarrToZarr
import unittest


class TestCreateKerchunk(unittest.TestCase):
    def test_find_files(self):
        regex = r'^.*.2d.*3[0-1]_19:.*$' # 30th and 31st at 1900UTC
        extensions = ['.nc']
        path = '/glade/campaign/collections/rda/data/d559000/wy1981/198101/'
        expected_output = ['/glade/campaign/collections/rda/data/d559000/wy1981/198101/wrf2d_d01_1981-01-30_19:00:00.nc',
                           '/glade/campaign/collections/rda/data/d559000/wy1981/198101/wrf2d_d01_1981-01-31_19:00:00.nc']
        files = create_kerchunk.find_files(path, regex, extensions)
        self.assertEqual(files,expected_output)

    def test_get_cluster(self):
        client = create_kerchunk.get_cluster('pbs', num_processes=10)
        print(client)
        client = create_kerchunk.get_cluster('Not a cluster')
        print(client)
        client = create_kerchunk.get_cluster('local', num_processes=5)
        print(client)
        client = create_kerchunk.get_cluster('single', num_processes=5)
        print(client)
        client = create_kerchunk.get_cluster('k8s', num_processes=5)
        print(client)

def test_combine():
    create_kerchunk.process_kerchunk_combine(directory='/glade/campaign/collections/rda/data/d559000/wy1981/198101/', output_directory='.', extensions=[], regex=r"^.*wrf2d.*-01.*$", output_filename="T2_P_198101.json", variables=['T2','P'], dry_run=False)

def test_create_mzz():
    path = '/glade/campaign/collections/rda/data/d559000/wy1981/198102/wrf3d_d01_1981-02-27_0*'
    path = '/glade/campaign/collections/rda/data/d633000/e5.oper.an.pl/194007/*_t*.nc'
    files = glob.glob(path)
    assert(len(files) == 31)
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
    #os.remove(output_fname)


def test_chunks():
    filename = '/gpfs/csfs1/collections/rda/data/d559000/wy1981/198101/wrf2d_d01_1981-01-31_19:00:00.nc'

def test_get_time_variable():
    filename = '/gpfs/csfs1/collections/rda/data/d559000/wy1981/198101/wrf2d_d01_1981-01-31_19:00:00.nc'
    assert create_kerchunk.get_time_variable(filename) == 'Time'
    filename = '/gpfs/csfs1/collections/rda/data/d633000/e5.oper.an.pl/194007/e5.oper.an.pl.128_130_t.ll025sc.1940071600_1940071623.nc'
    assert create_kerchunk.get_time_variable(filename) == 'time'
    filename = '/gpfs/csfs1/collections/rda/data/ds633.0/e5.oper.fc.sfc.accumu/196204/e5.oper.fc.sfc.accumu.128_144_sf.ll025sc.1962040106_1962041606.nc'
    assert create_kerchunk.get_time_variable(filename) == 'forecast_initial_time'

#test_create_mzz()
#files = test_find_files()
#print(files)
#test_combine()
#test_get_time_variable()


if __name__ == '__main':
    unittest.main()
