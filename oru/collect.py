import csv
import dataclasses
import os.path
import sys
from .gurobi import ModelInformation
from typing import Sequence
from .logging import CSVLog
import json
from itertools import chain

def collect_model_info(filelist : Sequence[str], output_file : str = None, strip_directory=True, strip_ext=True,
                       drop_fields=None):
    def prep_filename(fn):
        if strip_directory:
            fn = os.path.basename(fn)
        if strip_ext:
            fn, _ = os.path.splitext(fn)
        return fn


    if output_file is None:
        csv_file = sys.stdout
        closefile = False
    else:
        csv_file = open(output_file, 'w')
        closefile = True

    rows = []
    for f in filelist:
        with open(f, 'r') as fp:
            rows.append(json.load(fp))

    csv_fields = set(chain(*map(lambda r : r.keys(), rows)))

    if drop_fields is not None:
        csv_fields -= set(drop_fields)

    csv_fields = ['index'] + sorted(csv_fields)



    csv_writer = csv.DictWriter(csv_file, csv_fields, extrasaction='ignore')
    csv_writer.writeheader()

    for r, fname in zip(rows, filelist):
        r['index'] = prep_filename(fname)
        csv_writer.writerow(r)

    if closefile:
        csv_file.close()