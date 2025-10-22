#!/usr/bin/env python

import os, sys
import json
import ujson
import pdb
import argparse
import re
import codecs
import time

import dask
import kerchunk.hdf
from fsspec.implementations.local import LocalFileSystem
from pathlib import Path
from kerchunk.combine import MultiZarrToZarr
from dask_jobqueue import PBSCluster
from dask.distributed import Client, LocalCluster

### Global settings

# special keyword to indicate all variables should be separated
ALL_VARIABLES_KEYWORD = "ALL"
# global filesystem object
fs = LocalFileSystem()
# default fs.open(file, **so) arguments dictionary for writing kerchunk reference files
so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first')
# initial global dask client
_global_client = None


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
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--action', '-a',
        type=str,
        required=True,
        choices=['combine','sidecar'],
        nargs=None,
        metavar='<combine|sidecar>',
        help='Specify whether to create to combine references or create sidecar files.'
    )
    parser.add_argument(
        '--directory', '-d',
        type=str,
        nargs=None,
        metavar='<directory>',
        required=True,
        help="Directory to scan and create kerchunk reference files."
    )
    parser.add_argument(
        '--output_directory', '-o',
        type=str,
        nargs=None,
        metavar='<directory>',
        required=False,
        default='.',
        help="Directory to place output files"
    )
    parser.add_argument(
        '--filename', '-f',
        type=str,
        nargs=None,
        metavar='<output filename>',
        required=False,
        default='',
        help="Filename for output json."
    )
    parser.add_argument(
        '--extensions', '-e',
        type=str,
        required=False,
        nargs='+',
        metavar='<extension>',
        help='Only process files of this extension',
        default=[]
    )
    parser.add_argument(
        '--variables', '-v',
        type=str,
        required=False,
        nargs='+',
        metavar='<variable names>',
        help=(
        "Only gather specific variables.\n"+
        "Variable names are case sensitive.\n"+
        f"Use the special keyword '{ALL_VARIABLES_KEYWORD}' to separate all into individual files."
        ),
        default=[]
    )
    parser.add_argument(
        '--cluster', '-c',
        type=str,
        default="local",
        required=False,
        nargs=1,
        metavar='< PBS / single / local >',
        help="""Choose type of dask cluster to use.
        PBS - PBSCluster (defaults to 5 workers)
        single - singleThreaded
        local - localCluster (uses os.ncpus)
        """
    )
    parser.add_argument(
        '--dry_run', '-dr',
        action='store_true',
        required=False,
        help='Do a dry run of processing',
        default=[]
    )
    parser.add_argument(
        '--make_remote', '-mr',
        action='store_true',
        required=False,
        help='Additionally make a remote accessible copy of json',
        default=[]
    )

    def unescaped_str(arg_str):
        """Remove escape characters from argument string."""
        return arg_str.replace("\\\\","\\")

    parser.add_argument(
        '--regex', '-r',
        type=unescaped_str,
        required=False,
        nargs=None,
        metavar='<regular expression>',
        help='Combine references that match'
    )

    return parser


def get_cluster(
    cluster_setting,
    num_processes=5,
    local_directory_pbs: str = None,
    log_directory_pbs: str = None
):
    """Starts a cluster given option and create a global client

    Args:
        cluster (str): May be the following
            'PBS' - PBSCluster where 5 workers are used.
            'local' - LocalCluster where number of workers are os.ncpus().
            'single' - single threaded localCluster>
            'k8s' - KubeCluster
        num_processes (int): Number of processes/workers to use. Default 0-use cluster default

    Returns:
        dask.distributed.Client - Client object for all cluster types.
    """
    global _global_client

    cluster_setting = cluster_setting.lower()
    match cluster_setting:
        case "pbs":
            cluster = PBSCluster(
                job_name = 'gdex-kerchunk-hpc',
                cores = 1,
                memory = '4GiB',
                processes = 1,
                account = 'P43713000',
                local_directory = local_directory_pbs,
                log_directory = log_directory_pbs,
                resource_spec = 'select=1:ncpus=1:mem=4GB',
                queue = 'gdex',
                walltime = '10:00:00',
                interface = 'ext'
            )
            cluster.scale(jobs=num_processes)
        case 'single':
            cluster = LocalCluster(n_workers=1, threads_per_worker=1, processes=False)
        case 'k8s':
            try :
                # old version of dask_kubernetes
                from dask_kubernetes import KubeCluster
            except ImportError:
                # new version of dask_kubernetes
                from dask_kubernetes.operator import KubeCluster
            cluster = KubeCluster()
            cluster.scale(jobs=num_processes)
        case 'local':
            cluster = LocalCluster()
        case _: # default use localCluster
            cluster = LocalCluster()

    if _global_client is None:
        _global_client = cluster.get_client()

def cleanup_dask_client():
    """Cleanup global dask client."""
    global _global_client
    if _global_client is not None:
        _global_client.close()
        _global_client = None


def gen_json(file_url, write_json=False):
    """Generate kerchunk json structure for a single file.
    
    Parameters
    -----------
    file_url : str
        path to file to generate kerchunk json structure for
    write_json : bool
        whether to write the json structure to a sidecar file

    Returns
    --------
    json_struct : dict
        kerchunk json structure

    Notes
    -----
    - sidecar file will be named <file_url>.json
    - the function also checks if the sidecar file already exists
    - the function will write the sidecar file if it does not exist and
      write_json is True

    """
    file_basename = os.path.basename(file_url)
    outfile = f'{file_basename}.json'
    print(f'generating {outfile}')

    # skip build and just load and return existing json structure
    # TODO: check read construct validity
    if os.path.exists(outfile):
        print(f'{outfile} exists, skipping')
        with fs.open(outfile, 'rb') as f:
            json_struct = ujson.loads(f.read().decode())
        return json_struct

    # build json structure if not exists
    with fs.open(file_url, **so) as infile:
        # set vlen_encode to 'leave' to avoid issues with string variable (d640000)
        # set error to 'ignore' to skip over string decoding issues
        # check https://fsspec.github.io/kerchunk/reference.html#kerchunk.hdf.SingleHdf5ToZarr
        # for more details
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(
            infile,
            file_url,
            inline_threshold=366,
            vlen_encode='leave',
            error='ignore'
        )
        # year = file_url.split('/')[-1].split('.')[0]
        if write_json:
            with fs.open(outfile, 'wb') as f:
                print(f'writing {outfile}')
                f.write(ujson.dumps(h5chunks.translate()).encode())

        json_struct = h5chunks.translate()

    return json_struct

def create_directories(dirs, base_path='./'):
    """Create directories in dirs list if they do not exist."""
    for i in dirs:
        directory_path = os.path.join(base_path, i)
        # create directory if it does not exist
        if not os.path.exists(directory_path):
            os.mkdir(directory_path)
    return None

def matches_extension(filename, extensions):
    """Check if filename matches one of the extensions."""
    if len(extensions) == 0:
        return True
    for j in extensions:
        if re.match(f'.*{j}$', filename):
            return True
    return False

def process_kerchunk_sidecar(directory, output_directory='.', extensions=None, dry_run=False):
    """Traverse files in `directory` and create kerchunk sidecar files."""

    if extensions is None:
        extensions = []

    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Directory "{directory}" cannot be found')
        sys.exit(1)

    # change working directory to output directory
    try:
        os.stat(output_directory)
    except FileNotFoundError:
        print(f'Output directory "{output_directory}" cannot be found')
        sys.exit(1)
    os.chdir(output_directory)

    for cur_dir, child_dirs, files in os.walk(directory):
        # data file location information
        cur_dir_base = os.path.basename(os.path.normpath(cur_dir))

        # check if reference file location has a subdirectory with the same name as
        # the cur_dir_base at data file location
        try:
            os.stat(cur_dir_base)
            os.chdir(cur_dir_base)
        except FileNotFoundError:
            pass

        # run reference generation for each file (if file matches extension and exists)
        for f in files:
            if matches_extension(f, extensions):
                # print file being processed
                print(f)
                # create json reference sidecar file
                if not dry_run:
                    gen_json(os.path.join(cur_dir,f), write_json=True)

        if len(child_dirs) == 0:
            # if get to the end of the branch, go back up one level
            os.chdir('..')
        else:
            # create child directories
            create_directories(child_dirs)

def find_files(directory, regex, extensions):
    """Find matching files in directory."""
    all_files = []
    # Handle empty regex - match all files if regex is empty
    if regex:
        pattern = re.compile(regex)
    else:
        pattern = re.compile(".*")  # Match everything
    
    for cur_dir, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(cur_dir, file)
            if matches_extension(full_path, extensions) and pattern.match(full_path):
                all_files.append(full_path)
    return all_files

def get_time_variable(filename):
    """Get time variable name in the file
    Will try different methods for finding lat in decreasing authority.

    Parameters
    ----------
    filename : str
        path to file to examine
    
    Returns
    -------
    time_varname : str or None
        name of time variable if found, else None (need to check manually)
    """
    import xarray
    ds = xarray.open_dataset(filename)

    for key, value in ds.coords.items():
        if 'standard_name' in value.attrs and value.attrs['standard_name'] == 'time':
            return key
        if 'standard_name' in value.attrs and value.attrs['standard_name'] == 'forecast_reference_time':
            return key
        if 'long_name' in value.attrs and value.attrs['long_name'] == 'time':
            return key
        if 'short_name' in value.attrs and value.attrs['short_name'] == 'time':
            return key
        if key.lower() == 'time':
            return key
        if 'units' in value.attrs and 'minutes since' in value.attrs['units']:
            return key

    return None

def separate_vars(refs, var_names):
    """Extracts specific variables from reference files object."""
    # preset keep variables
    keep_values = set([
        '.zgroup',
        '.zattrs',
        'Time',
        'XLAT',
        'XLONG',
        'XLAT_U',
        'XLONG_U',
        'XLAT_V',
        'XLONG_V',
        'XTIME',
    ])
    # update keep variables with user specified variables in var_names
    keep_values.update(var_names)

    # process each reference file object
    # only keep variables in keep_values
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

# def separate_combine_write_all_vars(refs, var_names, make_remote=False):
#     """Extracts specific variables from refs.
#     """
#     updated_refs = separate_vars(refs, var_names)

#     print('combining')
#     mzz = MultiZarrToZarr(
#            updated_refs,
#            concat_dims=["time"],
#            #coo_map='QSNOW',
#         )
#     multi_kerchunk = mzz.translate()
#     write_kerchunk(output_directory, multi_kerchunk, regex, variables, output_filename, make_remote)

def write_combined_kerchunk(output_directory, multi_kerchunk, regex=None, output_filename="", make_remote=False):
    """Write kerchunk .json for combined kerchunk.

    Args:
        output_directory (str): Directory to  place json files.
        multi_kerchunk (list): list of Kerchunk dicts.
        regex (str): regex used to search over source files (used to guess output filename).
        output_filename (str): name of the output file.
        make_remote (bool): whether to make the output file remote.
    """

    if output_filename:
        if output_filename.strip()[-5:] != '.json':
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


def process_kerchunk_combine(
    directory,
    output_directory='.',
    extensions=None,
    regex=None,
    dry_run=False,
    variables=None,
    output_filename="",
    make_remote=False
):
    """Traverse files in `directory` and create kerchunk aggregated files."""

    # set default arguments
    if extensions is None:
        extensions = []
    if variables is None:
        variables = []

    # check if data directory exists
    try:
        os.stat(directory)
    except FileNotFoundError:
        print(f'Directory "{directory}" cannot be found')
        sys.exit(1)
    
    # find files to process
    files = find_files(directory, regex, extensions)
    print(f'Number of files: {len(files)}')

    time_varname = get_time_variable(files[0])
    # check if time variable name is found
    if time_varname is None:
        print('Could not determine time variable name')
        exit(1)
    
    lazy_results = []
    if dry_run:
        print(f'processing {files}')
        print(f'{len(files)} files to process')
        exit(1)
    all_refs = []
    for f in files:
        lazy_result = dask.delayed(gen_json)(f)
        lazy_results.append(lazy_result)
        # Split up large jobs
        if len(lazy_results) > 5000:
            start = time.time()
            print(f'Intermediate processing ({len(all_refs)}/{len(files)})')
            tmp_refs = dask.compute(*lazy_results)
            all_refs.extend(tmp_refs)
            lazy_results = []
            end = time.time()
            print(f'Done intermediate. Elapsed time ({end-start} seconds)')


    all_refs.extend(dask.compute(*lazy_results))

    # if len(variables) == 1 and variables[0] == ALL_VARIABLES_KEYWORD:
    #     separate_combine_write_all_vars(all_refs, variables, make_remote)
    #     exit(1)
    if len(variables) > 0:
        all_refs = separate_vars(all_refs, variables)

    print('combining')
    mzz = MultiZarrToZarr(
           all_refs,
           concat_dims=[time_varname],
           #coo_map='QSNOW',
        )
    multi_kerchunk = mzz.translate()
    write_combined_kerchunk(output_directory, multi_kerchunk, regex, output_filename, make_remote)


def main():
    """Entrypoint for command line application."""
    parser = _get_parser()
    print(sys.argv)
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    print(args)

    # initialize dask client
    get_cluster(
        cluster_setting = args.cluster,
        num_processes=5,
        local_directory_pbs=None,
        log_directory_pbs=None
    )

    if args.action == 'sidecar':
        process_kerchunk_sidecar(
            args.directory,
            args.output_directory,
            extensions=args.extensions,
            dry_run=args.dry_run
        )
    elif args.action == 'combine':
        process_kerchunk_combine(
            args.directory,
            args.output_directory,
            extensions=args.extensions,
            dry_run=args.dry_run,
            variables=args.variables,
            regex=args.regex,
            output_filename=args.filename,
            make_remote=args.make_remote
        )
    else:
        print(f'action type "{args.action}" not recognized')
        exit(1)

    # cleanup dask client
    cleanup_dask_client()

if __name__ == '__main__':
    main()
