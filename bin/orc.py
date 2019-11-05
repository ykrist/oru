#!/usr/bin/env python
import oru.collect
import oru.slurm
import os
import argparse
import textwrap
import subprocess
import sys
import tempfile
import fnmatch
import json
from collections import defaultdict

SBATCH_COMMAND=['sbatch']

def error(*args, exit=True, **kwargs):
    kwargs['file'] = sys.stderr
    print('error:', *args, **kwargs)
    if exit:
        sys.exit(1)

def collect_command(args):
    oru.collect.collect_model_info(filelist=args.files,
                                   output_file=args.output,
                                   drop_fields=args.drop,
                                   strip_ext=args.se,
                                   strip_directory=args.sd)

def sbatch_harray_command(args):
    for i in args.array_range:
        slurm_info_proc = subprocess.run(["python", args.target, '--slurminfo'] + args.target_args + [str(i)],
                                         text=True, stdout=subprocess.PIPE)
        if slurm_info_proc.returncode != 0:
            print(f"failed: TARGET return exit code: {slurm_info_proc.returncode:d}")
            sys.exit(1)
        else:
            try:
                slurm_info, other_info = oru.slurm.parse_slurm_info(slurm_info_proc.stdout)
            except Exception as e:
                print(f"failed: ", str(e))
                sys.exit(1)

        bash_script_contents = other_info['script'].format(
            python_script=args.target,
            job_index=i,
            time_limit=slurm_info['time']
        )

        bash_script_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        bash_script_file.file.write(bash_script_contents)
        bash_script_file.file.close()

        slurm_args = []
        for key,val in slurm_info.items():
            slurm_args.extend(("--"+key, val))

        command = SBATCH_COMMAND + slurm_args + [bash_script_file.name]

        if args.verbose:
            print(" ".join(command))
            print(bash_script_file.name.center(80, '-'))
            print(bash_script_contents)
            print("-"*80)

        if not args.dryrun:
            result = subprocess.run(command, stderr=subprocess.STDOUT)
            result.check_returncode()
            if result.returncode != 0:
                print(f"command `{' '.join(command)}` exit with nonzero status {result.returncode:d}:")
                os.remove(bash_script_file.name)
                sys.exit(1)

        os.remove(bash_script_file.name)

def _dict_recursive_update(target, other):
    for key,val in target.items():
        if key in other:
            other_val = other[key]
            if isinstance(val, dict) and isinstance(other_val, dict):
                target[key] = _dict_recursive_update(val, other_val)
            else:
                target[key] = other[key]

    for key, val in other.items():
        if key not in target:
            target[key] = other[key]
    return target

def _dict_nested_keys_to_dot_name(d, _prefix='', _pred=()):
    names = {}
    if len(_prefix) > 0:
        _prefix += "."
    for key in d:
        nested_key = _pred + (key,)
        dotname = _prefix + key
        if isinstance(d[key], dict):
            names.update(_dict_nested_keys_to_dot_name(d[key], _prefix=dotname, _pred=nested_key))
        names[nested_key] = dotname
    return names

def _dict_rec_delete(d, rec_key):
    if len(rec_key) == 1:
        del d[rec_key[0]]
    else:
        if rec_key[0] in d:
            _dict_rec_delete(d[rec_key[0]], rec_key[1:])

def _dict_rec_insert(d, rec_key, val):
    if len(rec_key) == 1:
        d[rec_key[0]] = val
    else:
        if rec_key[0] not in d:
            d[rec_key[0]] = {}
        _dict_rec_insert(d[rec_key[0]], rec_key[1:], val)

def _dict_rec_get(d, rec_key):
    if len(rec_key) == 1:
        return d[rec_key[0]]
    else:
        return _dict_rec_get(d[rec_key[0]], rec_key[1:])

def json_command(args):
    input_files = args.input
    if input_files.count('-') > 1:
        error('STDIN given multiple times.')

    data = dict()
    for f in input_files:
        if f == '-':
            new_data = json.loads(sys.stdin.read())
        else:
            with open(f, 'r') as fp:
                new_data = json.load(fp)
        _dict_recursive_update(data, new_data)

    if args.patterns is not None:
        dotnames = _dict_nested_keys_to_dot_name(data)
        dotnames_inv = {v : k for k,v in dotnames.items()}
        match = set()
        for pattern in args.patterns:
            match.update(map(lambda x : dotnames_inv[x], fnmatch.filter(dotnames_inv, pattern)))

        match = sorted(match, key=lambda x : (len(x),) + x)
        new_data = {}
        reckeys_by_length = defaultdict(set)
        for reckey in match:
            reckeys_by_length[len(reckey)].add(reckey)
        reckeys_by_length.default_factory = None

        for keylen in sorted(reckeys_by_length.keys()):
            for reckey in reckeys_by_length[keylen]:
                if keylen-1 in reckeys_by_length and reckey[:-1] in reckeys_by_length[keylen-1]:
                    continue
                val = _dict_rec_get(data, reckey)
                _dict_rec_insert(new_data, reckey, val)

        data = new_data

    if args.output is not None:
        with open(args.output, 'w') as fp:
            json.dump(data ,fp)
    else:
        print(json.dumps(data, indent='\t'))



if __name__ == '__main__':

    main_p = argparse.ArgumentParser()
    sp = main_p.add_subparsers(required=True)

    p_collect = sp.add_parser("collect")
    p_collect.add_argument("files", nargs="+", type=str,
                           help="List of input JSON files.")
    p_collect.add_argument("-o", "--output", type=str, default=None,
                           help="Save the result to a file rather than printing to STDOUT.")
    p_collect.add_argument("-se", action="store_true",
                           help="Strip extenstion when converting input filename to CSV index.")
    p_collect.add_argument("-sd", action="store_true",
                           help="Strip directories when converting input filename to CSV index")
    p_collect.add_argument("-d","--drop", type=str, nargs='+',
                           help="Drop the following fields from input files.")
    p_collect.set_defaults(func=collect_command)

    p_sbatch_harray = sp.add_parser("sbatch_harray")
    p_sbatch_harray.add_argument("-v", "--verbose", action='store_true')
    p_sbatch_harray.add_argument("--dryrun", action='store_true')
    p_sbatch_harray.add_argument("target",
                                 help="target file to run.  The TARGET must accept a --slurmid option, and when given this option, "
                        "should print a JSON string to STDOUT containing all the necessary information (see TODO) before "
                        "exiting.  Furthermore, the last"
                        " argument of TARGET must be an integer; this is what is passed to the TARGET based on the "
                        "supplied ARRAY_RANGE.  Thus TARGET must have usage as follows: `target [args] [--slurmid] "
                        "idx`.")
    p_sbatch_harray.add_argument("target_args", nargs="*",
                                 help="Arguments to pass through to TARGET.  If optional arguments are being passed to TARGET "
                        "(arguments beginning with `-`, then this list must be prefixed with `--`.")
    p_sbatch_harray.add_argument("array_range", help="Array indices to run over, each array index is passed separately to TARGET.",
                                 type=oru.slurm.array_range)
    p_sbatch_harray.set_defaults(func=sbatch_harray_command)

    p_json = sp.add_parser("json")
    p_json.add_argument("input", type=str, nargs='+',
                        help='input JSON file, set to - for STDIN.  Multiple input files can be specified: they'
                             'will be merged recursively before any operations.')
    p_json.add_argument("-o", "--output", type=str,
                        help="output to file instead of STDOUT (default).")
    p_json.add_argument("-f", "--field", type=str, dest='patterns', action='append',
                        help="Extract fields from JSON.  Use a `.` to access nested fields.  "
                             "Standard wildcard matching is supported.  Use this option multiple times to specify"
                             " several patterns at once")
    p_json.add_argument("-p", "--print", action='store_true',
                        help="Pretty print the output rather than output a JSON string (the default).")
    p_json.set_defaults(func=json_command)

    if len(sys.argv) > 1:
        args = main_p.parse_args(sys.argv[1:])
        args.func(args)
    else:
        main_p.print_usage()
        sys.exit(1)



