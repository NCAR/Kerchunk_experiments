#!/usr/bin/env python

import os, sys
import ujson
import pdb
import argparse
import re

from fsspec.implementations.local import LocalFileSystem
from pathlib import Path
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr


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

    # Global arguments
    parser.add_argument('--directory', '-d',
                        type=str,
                        nargs=1,
                        metavar='<directory>',
                        required=True,
                        help="Directory to scan and create kerchunk reference files")
    parser.add_argument('--output_location', '-o',
                        type=str,
                        nargs=1,
                        metavar='<directory>',
                        required=False,
                        default='.',
                        help="Directory to place output files")

    parser.add_argument('--extensions', '-e',
                        type=str,
                        required=False,
                        nargs='+',
                        metavar='<extension>',
                        help='Only process files of this extension',
                        default=[])


    parser.add_argument('--dry_run', '-dr',
                        action='store_true',
                        required=False,
                        help='Do a dry run of processing',
                        default=[])

    # Mutually exclusive commands
    actions_parser = parser.add_subparsers(title='Actions',
            help='Use `tool [command] -h` for more info on command')
    actions_parser.required = True
    actions_parser.dest = 'Action'

    # Actions
    sidecar_parser = actions_parser.add_parser(
            "sidecar",
            #aliases=['sidecar', 'sc'],
            help='Create sidecar files',
            description='creates sidecar files')


    combine_parser = actions_parser.add_parser(
            "combine",
            #aliases=['combine', 'cb'],
            help='Combines references from files',
            description='Combines references into a single file.')
    combine_parser.add_argument('--regex', '-r',
                        type=str,
                        required=False,
                        nargs=1,
                        metavar='<regular expression>',
                        help='Combine references that match')


    return parser

fs = LocalFileSystem()
so = dict(mode='rb', anon=True, default_fill_cache=False, default_cache_type='first')

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
        return h5chunks.translate()


def main():
    """Entrypoint for command line application."""
    parser = _get_parser()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    print(args)
    if args.Action == 'sidecar':
        process_kerchunk_sidecar(args.directory[0], args.output_location[0],
                     extensions=args.extensions,
                     dry_run=args.dry_run)
    if args.Action == 'combine':
        process_kerchunk_sidecar(args.directory[0], args.output_location[0],
                     extensions=args.extensions,
                     dry_run=args.dry_run)


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


if __name__ == '__main__':
    main()
