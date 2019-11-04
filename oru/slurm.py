import argparse
import json
import re
import sys
import os
import cerberus
import hashlib

SLURM_INFO_OPTIONAL_FIELDS = [
    "job-name",
    "time",
    "mail-user",
    "mail-type",
    "nodes",
    "tasks-per-node",
    "mem",
    "cpus-per-task",
    "out",
    "err",
    "constraint",
]
SLURM_INFO_REQUIRED_FIELDS = [
    "script"
]

class BaseSlurmResources:
    def __init__(self, job_index):
        self.job_index = job_index
        self.logdir = '.'

    @property
    def nodes(self) -> str:
        return "1"

    @property
    def tasks_per_node(self) -> str:
        return "1"

    @property
    def mail_user(self) -> str:
        return None

    @property
    def mail_type(self) -> str:
        return "FAIL"

    @property
    def memory(self) -> str:
        """Should return memory to be allocated in MB"""
        return None

    @property
    def time(self) -> str:
        """Should return time to be allocated in seconds"""
        return None

    @property
    def cpus(self) -> str:
        """Should return time to be allocated in seconds"""
        return None

    @property
    def stdout_log(self) -> str:
        return f'{self.logdir}/{self.name}.out'

    @property
    def stderr_log(self) -> str:
        return f'{self.logdir}/{self.name}.err'

    @property
    def name(self) -> str:
        return f'%j-{self.job_index:d}'

    @property
    def constraints(self) -> str:
        return None

    @property
    def script(self):
        raise NotImplementedError


    def get_slurminfo_json_string(self):

        cl_opts = {
            "time" : self.time,
            "job-name" : self.name,
            "out" : self.stdout_log,
            "err" : self.stderr_log,
            "mem" : self.memory,
            "cpus-per-task" : self.cpus,
            "nodes" : self.nodes,
            "tasks-per-node" : self.tasks_per_node,
            "mail-user" : self.mail_user,
            "mail-type" : self.mail_type,
            "constraint" : self.constraints,
            "script" : self.script,
        }
        cl_opts = dict(filter(lambda kv : kv[1] is not None, cl_opts.items()))
        if "mail-user" not in cl_opts and "mail-type" in cl_opts:
            del cl_opts['mail-type']

        return json.dumps(cl_opts, indent='\t')


    @staticmethod
    def format_time(seconds : int) -> str:
        minutes = seconds//60
        seconds -= minutes*60
        hours = minutes//60
        minutes -= hours*60
        days = hours//24
        hours -= days*24
        return f'{days:d}-{hours:02d}:{minutes:02d}:{seconds:02d}'


def parse_slurm_info(jsonstr):
    info = json.loads(jsonstr)
    info_fields = set(info.keys())
    missing_req_fields= set(SLURM_INFO_REQUIRED_FIELDS) - info_fields
    if len(missing_req_fields) > 0:
        raise KeyError(f"Missing required JSON fields:\n\t" + "\n\t".join(missing_req_fields))
    unknown_fields = info_fields - set(SLURM_INFO_REQUIRED_FIELDS + SLURM_INFO_OPTIONAL_FIELDS)
    if len(unknown_fields) > 0:
        raise KeyError(f"Unknown JSON fields:\n\t" + "\n\t".join(unknown_fields))
    non_slurm_info = {key : info.pop(key) for key in ["script"]}
    return info, non_slurm_info


def array_range(str):
    tokens = list(filter(lambda x : len(x) > 0, str.split(",")))
    indices = set()
    for t in tokens:
        m =re.fullmatch(r"(?P<start>\d+)-(?P<stop>\d+)(:(?P<step>\d+))?", t)
        if m is None:
            indices.add(int(t))
        else:
            groups = m.groupdict(default=1)
            indices.update(range(int(groups['start']), int(groups['stop'])+1, int(groups['step'])))
    return sorted(indices)


def _validate_and_raise(validator, document, schema=None):
    valid = validator.validate(document, schema)
    if not valid:
        raise ValueError(validator.errors)
    return validator.document


def _remove_delayed(d):
    d = d.copy()
    for key in list(d.keys()):
        if d[key].pop("delayed", False):
            del d[key]
    return d


class Experiment:
    INPUTS = {"index" : {"type" : "int", "min" : 0}}
    OUTPUTS = None
    PARAMETERS = {}
    RESOURCES = None
    ROOT_PATH = "."
    PATH_SEP = "_"
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("--slurminfo", action='store_true')
    PARSER.add_argument("index", metavar="IDX", type=int)


    def __init__(self, inputs, outputs, parameters=None):
        if parameters is None:
            parameters = dict()

        v = cerberus.Validator(require_all=True)

        self.inputs = _validate_and_raise(v, inputs, _remove_delayed(self.INPUTS))
        self.outputs = _validate_and_raise(v, outputs, _remove_delayed(self.OUTPUTS))
        self.parameters = _validate_and_raise(v, parameters, _remove_delayed(self.PARAMETERS))
        self._directory = None
        self._parameter_string = None

        self.define_delayed()

        self.inputs = _validate_and_raise(v, self.inputs, self.INPUTS)
        self.parameters = _validate_and_raise(v, self.parameters, self.PARAMETERS)
        self.outputs = _validate_and_raise(v, self.outputs, self.OUTPUTS)

        if self.RESOURCES is not None:
            self.resources = self.RESOURCES(self)
        else:
            self.resources = None

    @classmethod
    def from_cl_args(cls, args=None):
        if args is None:
            args = sys.argv[1:]
        args = vars(cls.PARSER.parse_args(args))
        slurminfo = args.pop("slurminfo")
        experiment = cls(args, {})
        if slurminfo:
            if experiment.resources is None:
                raise  NotImplementedError
            print(experiment.resources.get_slurminfo_json_string())
            sys.exit(0)
        return experiment

    def define_delayed(self):
        """
        Any inputs/outputs/parameters with the `delayed` rule set must be specified here.
        """
        pass

    @property
    def parameter_string(self):
        if self._parameter_string is None:
            s =  json.dumps(self.parameters, sort_keys=True)
            self._parameter_string = hashlib.blake2b(s.encode(), digest_size=8).hexdigest()
        return self._parameter_string

    @property
    def input_string(self):
        return self.PATH_SEP.join(str(self.inputs[iname]) for iname in sorted(self.inputs.keys()))

    @property
    def directory(self):
        if self._directory is None:
            self._directory = os.path.join(self.ROOT_PATH, self.parameter_string)
            os.makedirs(self._directory, exist_ok=True)
            paramsfile = os.path.join(self._directory, "parameters.json")
            if not os.path.exists(paramsfile):
                with open(paramsfile, 'w') as fp:
                    json.dump(self.parameters, fp, indent='\t')

        return self._directory

    def get_output_path(self, suffix):
        return os.path.join(self.directory, self.input_string + self.PATH_SEP + suffix)

