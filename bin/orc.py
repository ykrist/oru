from oru.collect import *
import os
import argparse

def collect_command(args):
    collect_model_info(filelist=args.files, output_file=args.output, strip_ext=args.se, strip_directory=args.sd)

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
    p_collect.set_defaults(func=collect_command)

    if len(sys.argv) > 1:
        args = main_p.parse_args(sys.argv[1:])
        args.func(args)
    else:
        main_p.print_usage()
        sys.exit(1)



