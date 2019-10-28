#!/usr/bin/env python
import oru.collect
import oru.slurm
import os
import argparse
import textwrap
import subprocess
import sys
import tempfile

SBATCH_COMMAND=['sbatch']

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




    if len(sys.argv) > 1:
        args = main_p.parse_args(sys.argv[1:])
        args.func(args)
    else:
        main_p.print_usage()
        sys.exit(1)



