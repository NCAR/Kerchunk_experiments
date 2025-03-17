#!/usr/bin/env python

import os, sys
import ujson
import pdb
import argparse
import re
import codecs

import dask
import kerchunk.hdf
from fsspec.implementations.local import LocalFileSystem
from pathlib import Path
from kerchunk.combine import MultiZarrToZarr
from dask_jobqueue import PBSCluster
from dask.distributed import Client, LocalCluster

ALL_VARIABLES_KEYWORD = "ALL"

def _get_parser():
    """Creates and returns parser object.
    Returns:
        (argparse.ArgumentParser): Parser object from which to parse arguments.
    """
    description = "Creates kerchunk sidecar files of an entire directory structure."
    prog_name = sys.argv[0] if sys.argv[0] != '' else 'create_kerchunk_sidecar'
    parser = argparse.ArgumentParser(
            prog=prog_name,
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--action', '-a',
                        type=str,
                        required=True,
                        choices=['combine','sidecar'],
                        nargs=None,
                        metavar='<combine|sidecar>',
                        help='Specify whether to create to combine references or create sidecar files.')
    parser.add_argument('--directory', '-d',
                        type=str,
                        nargs=None,
                        metavar='<directory>',
                        required=True,
                        help="Directory to scan and create kerchunk reference files.")
    parser.add_argument('--output_directory', '-o',
                        type=str,
                        nargs=None,
                        metavar='<directory>',
                        required=False,
                        default='.',
                        help="Directory to place output files")
    parser.add_argument('--filename', '-f',
                        type=str,
                        nargs=None,
                        metavar='<output filename>',
                        required=False,
                        default='',
                        help="Filename for output json.")
    parser.add_argument('--extensions', '-e',
                        type=str,
                        required=False,
                        nargs='+',
                        metavar='<extension>',
                        help='Only process files of this extension',
                        default=[])
    parser.add_argument('--variables', '-v',
                        type=str,
                        required=False,
                        nargs='+',
                        metavar='<variable names>',
                        help=f"""Only gather specific variables.
                        Variable names are case sensitive.

                        Use the special keyword '{ALL_VARIABLES_KEYWORD}' to separate all into individual files.""",
                        default=[])

    parser.add_argument('--cluster', '-c',
                        type=str,
                        default="local",
                        required=False,
                        nargs=1,
                        metavar='< PBS / single / local >',
                        help=f"""Choose type of dask cluster to use.
                        PBS - PBSCluster (defaults to 5 workers)
                        single - singleThreaded
                        local - localCluster (uses os.ncpus)
                        """,
                        )

    parser.add_argument('--dry_run', '-dr',
                        action='store_true',
                        required=False,
                        help='Do a dry run of processing',
                        default=[])

    parser.add_argument('--make_remote', '-mr',
                        action='store_true',
                        required=False,
                        help='Additionally make a remote accessible copy of json',
                        default=[])

    parser.add_argument('--regex', '-r',
                        type=unescaped_str,
                        required=False,
                        nargs=None,
                        metavar='<regular expression>',
                        help='Combine references that match')

    return parser


def unescaped_str(arg_str):
    return arg_str.replace("\\\\","\\")

fs = LocalFileSystem()
so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first')


def main():
    """Entrypoint for command line application."""
    parser = _get_parser()
    print(sys.argv)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    print(args)
    if args.action == 'sidecar':
        process_kerchunk_sidecar(args.directory, args.output_directory,
                     extensions=args.extensions,
                     dry_run=args.dry_run)
    elif args.action == 'combine':
        process_kerchunk_combine(args.directory, args.output_directory,
                     extensions=args.extensions,
                     dry_run=args.dry_run,
                     variables=args.variables,
                     regex=args.regex,
                     output_filename=args.filename,
                     make_remote=args.make_remote)
    else:
        print(f'type "{args.action}" not recognized')
        exit(1)


def gen_json(file_url, write_json=False):
    print(f'generating {file_url}')
    with fs.open(file_url, **so) as infile:
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(infile, file_url, inline_threshold=366 )

        year = file_url.split('/')[-1].split('.')[0]
        file_basename = os.path.basename(file_url)
        outfile = f'{file_basename}.json'
        if write_json:
            with fs.open(outfile, 'wb') as f:
                print(f'writing {outfile}')
                f.write(ujson.dumps(h5chunks.translate()).encode());
        json_struct = h5chunks.translate()


        return json_struct



def create_directories(dirs, base_path='./'):
    for i in dirs:
        os.mkdir(os.path.join(base_path, i))


def matches_extension(filename, extensions):
    if len(extensions) == 0:
        return True
    for j in extensions:
        if re.match(f'.*{j}$', filename):
            return True
    return False

def get_cluster(cluster, num_processes=5):
    """Starts a cluster given option.

    Args:
        cluster (str): May be the following
                       'PBS' - PBSCluster where 5 workers are used.
                       'local' - LocalCluster where number of workers are os.ncpus().
                       'single' - single threaded localCluster>
                       'k8s' - KubeCluster
        num_processes (int): Number of processes/workers to use. Default 0-use cluster default

    Returns:
        dask.distributed.Client - Client object
    """
    cluster = cluster.lower()
    match cluster:
        case "pbs":
            cluster = PBSCluster(
                job_name = 'dask-wk24-hpc',
                cores = 1,
                memory = '4GiB',
                processes = 1,
                account = 'P43713000',
                local_directory = '/gpfs/csfs1/collections/rda/scratch/rpconroy/dask/spill',
                log_directory = '/gpfs/csfs1/collections/rda/scratch/rpconroy/dask',
                resource_spec = 'select=1:ncpus=1:mem=4GB',
                queue = 'rda@casper-pbs',
                walltime = '10:00:00',
                interface = 'ext'
                )
            cluster.scale(jobs=num_processes)
        case 'single':
            cluster = LocalCluster(n_workers=1, threads_per_worker=1, processes=False)
        case 'k8s':
            from dask_kubernetes import KubeCluster
            cluster = KubeCluster()
            cluster.scale(jobs=num_processes)
        case _: # default use localCluster
            cluster = LocalCluster()


    client = cluster.get_client()

def process_kerchunk_sidecar(directory, output_directory='.', extensions=[], dry_run=False):
    """Traverse files in `directory` and create kerchunk sidecar files."""

    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Directory "{directory}" cannot be found')
        sys.exit(1)

    os.chdir(output_directory)

    for _dir in os.walk(directory):
        cur_dir = _dir[0]
        child_dirs = _dir[1]
        files = _dir[2]
        cur_dir_base = os.path.basename(os.path.normpath(cur_dir))
        try:
            os.stat(cur_dir_base)
            os.chdir(cur_dir_base)
        except FileNotFoundError:
            pass

        for f in files:
           if matches_extension(f, extensions):
               print(f)
               if not dry_run:

                   gen_json(os.path.join(cur_dir,f), write_json=True)

        if len(child_dirs) == 0:
            os.chdir('..')
        else:
            create_directories(child_dirs)

def find_files(directory, regex, extensions):
    """Find matching files in directory."""
    all_files = []
    pattern = re.compile(regex)
    for _dir in os.walk(directory):
        cur_dir = _dir[0]
        child_dirs = _dir[1]
        files = _dir[2]
        for _file in files:
            full_path = os.path.join(cur_dir, _file)
            if matches_extension(full_path, extensions) and pattern.match(full_path):
                all_files.append(full_path)
    return all_files

def get_time_variable(filename):
    """Get time Variable.
    Will try different methods for finding lat in decreasing authority.
    """
    import xarray
    ds = xarray.open_dataset(filename)

    for k,v in ds.coords.items():
        if 'standard_name' in v.attrs and v.attrs['standard_name'] == 'time':
            return k
        if 'standard_name' in v.attrs and v.attrs['standard_name'] == 'forecast_reference_time':
            return k
        if 'long_name' in v.attrs and v.attrs['long_name'] == 'time':
            return k
        if 'short_name' in v.attrs and v.attrs['short_name'] == 'time':
            return k
        if k.lower() == 'time':
            return k
        if 'units' in v.attrs and 'minutes since' in v.attrs['units']:
            return k
    return 'Time'

def separate_vars(refs, var_names):
    """Extracts specific variables from refs.
    """
    # for variables
    keep_values = set(['.zgroup', '.zattrs',
                       'Time',
                       'XLAT',
                       'XLONG',
                       'XLAT_U',
                       'XLONG_U',
                       'XLAT_V',
                       'XLONG_V',
                       'XTIME',
                       ])
    keep_values.update(var_names)
    updated_refs = []
    for ref in refs:
        new_json = {}
        for i in ref['refs']:
            varname = i.split('/')[0]
            if varname in keep_values:
                new_json[i] = ref['refs'][i]
        ref['refs'] = new_json
        updated_refs.append(ref)
    return updated_refs

def separate_combine_write_all_vars(refs, make_remote=False):
    """Extracts specific variables from refs.
    """
    import xarray
    # for variables
    keep_values = set(['.zgroup', '.zattrs',
                       'Time',
                       'XLAT',
                       'XLONG',
                       'XLAT_U',
                       'XLONG_U',
                       'XLAT_V',
                       'XLONG_V',
                       'XTIME',
                       ])
    keep_values.update(var_names)
    updated_refs = []
    for ref in refs:
        new_json = {}
        for i in ref['refs']:
            varname = i.split('/')[0]
            if varname in keep_values:
                new_json[i] = ref['refs'][i]
        ref['refs'] = new_json
        updated_refs.append(ref)

    print('combining')
    mzz = MultiZarrToZarr(
           all_refs,
           concat_dims=["time"],
           #concat_dims=["time"],
           #coo_map='QSNOW',
        )
    multi_kerchunk = mzz.translate()
    write_kerchunk(output_directory, multi_kerchunk, regex, variables, output_filename, make_remote)

def write_kerchunk(output_directory, multi_kerchunk, regex="", variable="", output_filename="", make_remote=False):
    """Write kerchunk .json record

    Args:
        output_directory (str): Directory to  place json files.
        multi_kerchunk (list): list of Kerchunk dicts.
        regex (str): regex used to search over source files (used to guess output filename).
        variable (str):
        output_filename (str)
        make_remote (bool)



    """

    if output_filename:
        if output_filename[-5] != '.json':
            output_filename = output_filename + '.json'
        output_fname = os.path.join(output_directory, output_filename)
    elif regex:
        guessed_filename = regex.replace('*','').replace('.','').replace('$','').replace('^','').replace('[','').replace(']','')
        output_fname = os.path.join(output_directory, guessed_filename)
    else:
        output_fname =  os.path.join(output_directory, 'combined_kerchunk.json')

    with open(f"{output_fname}", "wb") as f:
        f.write(ujson.dumps(multi_kerchunk).encode())

    if make_remote:
        import convert_ref_file_loc
        convert_ref_file_loc.main(output_fname, output_fname.replace('.json','-remote.json'))


def process_kerchunk_combine(directory, output_directory='.', extensions=[], regex="", dry_run=False, variables=[], output_filename="", make_remote=False):
    """Traverse files in `directory` and create kerchunk aggregated files."""
    number_of_workers=5
    cluster = PBSCluster(
            job_name = 'dask-wk24-hpc',
            cores = 1,
            memory = '4GiB',
            processes = 1,
            account = 'P43713000',
            local_directory = '/gpfs/csfs1/collections/rda/scratch/rpconroy/dask/spill',
            log_directory = '/gpfs/csfs1/collections/rda/scratch/rpconroy/dask',
            resource_spec = 'select=1:ncpus=1:mem=4GB',
            queue = 'rda@casper-pbs',
            walltime = '10:00:00',
            interface = 'ext'
        )
    cluster.scale(jobs=number_of_workers)
    client = Client(cluster)
    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Directory "{directory}" cannot be found')
        sys.exit(1)
    files = find_files(directory, regex, extensions)
    print(f'Number of files: {len(files)}')
    time_varname = get_time_variable(files[0])
    lazy_results = []
    if dry_run:
        print(f'processing {files}')
        print(f'{len(files)} files to process')
        exit(1)
    for f in files:
        lazy_result = dask.delayed(gen_json)(f)
        lazy_results.append(lazy_result)
    all_refs = dask.compute(*lazy_results)

    if len(variables) == 1 and variables[0] == ALL_VARIABLES_KEYWORD:
        separate_combine_write_all_vars()
        exit(1)
    elif len(variables) > 0:
        all_refs = separate_vars(all_refs, variables)

    print('combining')
    mzz = MultiZarrToZarr(
           all_refs,
           concat_dims=[time_varname],
           #coo_map='QSNOW',
        )
    multi_kerchunk = mzz.translate()
    write_kerchunk(output_directory, multi_kerchunk, regex, variables, output_filename, make_remote)

if __name__ == '__main__':
    main()
