#!/usr/bin/env python
import sys
import os
import json
import xarray
import time
import dask
import pdb
import fsspec
from kerchunk.combine import MultiZarrToZarr

def separate(filename):
    refs = json.load(open(filename))
    print('refs open')
    refs = refs['refs']
    var_names = list(refs.keys())
    unique_vars = set()
    for v in var_names:
        unique_vars.add(v.split('/')[0])
    var_types = get_var_types(filename)
    var_types = reshuffle_vars(var_types)
    new_refs = separate_primary_vars(var_types, refs)
    write_refs(new_refs, filename)

def write_refs(all_refs, filename):
    """Take collection of refs and write new json"""
    version = 1
    for refname, refs in all_refs.items():
        new_json = {'version':version}
        new_json['refs'] = refs
        fh = open(filename+'.'+refname+'.json', 'w')
        json.dump(new_json, fh)
        fh.close()


def separate_primary_vars(var_types, refs):
    """Separate out each variable into thier own reference file."""
    all_refs = {}
    for var_str in var_types['data_vars']:
        all_refs[var_str] = {}
        for ref in refs.keys():
            if var_str in ref:
                all_refs[var_str][ref] = refs[ref]
    return all_refs


def reshuffle_vars(var_types):
    response = ''
    while response != 'done':
        print('Coordinates:')
        [print(f'\t{x}') for x in var_types['coords']]
        print('Data Variables:')
        [print(f'\t{x}') for x in var_types['data_vars']]
        print('Type a var to move OR inspect [var] OR done')
        response = input().strip().strip('\n')
        print(f'"{response}"')
        if response != 'done':
            if 'inspect' in response:
                var = var_types['ds'].variables[response.split(' ')[1]]
                print(var)
            elif response in var_types['coords']:
                var_types['data_vars'].add(response)
                var_types['coords'].remove(response)
            elif response in var_types['data_vars']:
                var_types['coords'].add(response)
                var_types['data_vars'].remove(response)
            else:
                print(f'{response} not a coord or data var. Try again or type "done"')
                time.sleep(1)
    return var_types

def get_var_types(filename):
    """Return variable names that are primary or coordinates.

    Returns:
        (dict): key 'coords' - coordinate variables. key 'data_vars' - data variable names
    """
    print('opening local kerchunk')
    ds = open_local_kerchunk(filename)
    data_vars = set(ds.data_vars.keys())
    coords = set(ds.coords.keys())
    return {'coords':coords,'data_vars':data_vars, 'ds':ds}

def open_local_kerchunk(filename):
    fs = fsspec.filesystem('reference', fo=filename)
    m = fs.get_mapper('')
    ds = xarray.open_dataset(m, engine='zarr', backend_kwargs={'consolidated':False})
    return ds

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(f'Usage: {sys.argv[0]} [filename]')
        exit(1)
    filename = sys.argv[1]
    separate(filename)

