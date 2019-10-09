import csv
import dataclasses
import os.path
import sys
from .gurobi import ModelInformation
from typing import Sequence



def collect_model_info(filelist : Sequence[str], output_file : str = None, strip_directory=True, strip_ext=True):
    def prep_filename(fn):
        if strip_directory:
            fn = os.path.basename(fn)
        if strip_ext:
            fn, _ = os.path.splitext(fn)
        return fn
    data = ModelInformation.from_json(filelist[0])
    data = dataclasses.asdict(data)
    csv_fields = ['name'] + sorted(data.keys())
    data['name'] = prep_filename(filelist[0])

    if output_file is None:
        csv_file = sys.stdout
        closefile = False
    else:
        csv_file = open(output_file, 'w')
        closefile = True

    csv_writer = csv.DictWriter(csv_file, csv_fields)
    csv_writer.writeheader()
    csv_writer.writerow(data)

    for f in filelist[1:]:
        data = ModelInformation.from_json(f)
        data = dataclasses.asdict(data)
        data['name'] = prep_filename(f)
        csv_writer.writerow(data)

    if closefile:
        csv_file.close()