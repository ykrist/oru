import argparse
import json
import re
import sys
import os
import cerberus
import hashlib
from typing import Dict, Tuple

SLURM_INFO_OPTIONAL_FIELDS = (
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
)
SLURM_INFO_REQUIRED_FIELDS = (
    "script",
)

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


def _remove_derived(d):
    d = d.copy()
    for key in list(d.keys()):
        if d[key].pop("derived", False):
            del d[key]
    return d

_CERBERBUS_TYPE_TO_PYTHON_TYPE = {
 "integer" : int,
 "float" : float,
 "boolean" : bool,
 "string" : str,
 "number" : float
}

class Experiment:
    INPUTS = {"index" : {"type" : "integer", "min" : 0}}
    OUTPUTS = None
    PARAMETERS = {}
    ROOT_PATH = "."
    PATH_SEP = "_"

    def __init__(self, inputs, outputs, parameters=None):
        if parameters is None:
            parameters = dict()

        v = cerberus.Validator(require_all=True)

        self.inputs = _validate_and_raise(v, inputs, _remove_derived(self.INPUTS))
        self.outputs = _validate_and_raise(v, outputs, _remove_derived(self.OUTPUTS))
        self.parameters = _validate_and_raise(v, parameters, _remove_derived(self.PARAMETERS))
        self._directory = None
        self._parameter_string = None

        self.define_derived()

        self.inputs = _validate_and_raise(v, self.inputs, self.INPUTS)
        self.parameters = _validate_and_raise(v, self.parameters, self.PARAMETERS)
        self.outputs = _validate_and_raise(v, self.outputs, self.OUTPUTS)

    @classmethod
    def from_cl_args(cls, args=None):
        """Alternate constructor."""
        p = argparse.ArgumentParser()
        cl_args = cls.get_parser_arguments()
        for pargs, pkwargs in cl_args.values():
            p.add_argument(*pargs, **pkwargs)
        if args is None:
            args = sys.argv[1:]
        inputs = vars(p.parse_args(args))
        parameters = {pname : inputs.pop(pname) for pname in cls.PARAMETERS}
        slurminfo = inputs.pop("slurminfo")
        experiment = cls(inputs, {}, parameters)
        if slurminfo:
            print(experiment.get_slurminfo_json_string())
            sys.exit(0)
        return experiment

    def define_derived(self):
        """
        Any inputs/outputs/parameters with the `derived` rule set must be defined here.
        """
        pass

    @property
    def parameter_string(self):
        """A string which should be filesystem friendly and unique for unique parameters. Note this is
        technically not unique, but the chances of a hash collision is about 10**10 times smaller than me winning lotto"""
        if self._parameter_string is None:
            s =  json.dumps(self.parameters, sort_keys=True)
            self._parameter_string = hashlib.blake2b(s.encode(), digest_size=8).hexdigest()
        return self._parameter_string

    @property
    def input_string(self):
        """A string which should be filesystem friendly and unique for unique inputs."""
        return self.PATH_SEP.join(str(self.inputs[iname]) for iname in sorted(self.inputs.keys()))

    @property
    def directory(self):
        """Directory in which experiment results shall be placed."""
        if self._directory is None:
            self._directory = os.path.join(self.ROOT_PATH, self.parameter_string)
            os.makedirs(self._directory, exist_ok=True)
            paramsfile = os.path.join(self._directory, "parameters.json")
            if not os.path.exists(paramsfile):
                with open(paramsfile, 'w') as fp:
                    json.dump(self.parameters, fp, indent='\t')
        return self._directory

    def get_output_path(self, suffix):
        """Output files should be created using a file path obtained from this method."""
        if not suffix.startswith('.'):
            suffix = self.PATH_SEP + suffix
        return os.path.join(self.directory, self.input_string + suffix)

    @classmethod
    def get_parser_arguments(cls) -> Dict[str, Tuple[Tuple, Dict]]:
        """Using the non-derived inputs and parameters, create an ArgumentParser to parse these from the command line.
        This should return a dictionary mapping an input/parameter name to args and kwargs, which will be
         to ArgumentParser.add_argument (see argparse docs)"""
        cl_arguments =  {"slurminfo" : (("--slurminfo",), {'action':'store_true'})}
        for name, rules in cls.INPUTS.items():
            if rules.get('derived', False):
                continue
            kwargs = {}
            argtype = rules.get('type', None)

            if argtype in _CERBERBUS_TYPE_TO_PYTHON_TYPE:
                kwargs["type"] = _CERBERBUS_TYPE_TO_PYTHON_TYPE[argtype]

            cl_arguments[name] = ((name,), kwargs)

        for name, rules in cls.PARAMETERS.items():
            if rules.get('derived', False):
                continue

            argname = "--" + name.replace("_", "-")
            kwargs = {"dest" : name}
            if 'default' in rules:
                kwargs['default'] = rules['default']
                kwargs['required'] = False
            else:
                kwargs['required'] = True
            argtype = rules.get('type', None)
            if argtype in _CERBERBUS_TYPE_TO_PYTHON_TYPE:
                kwargs['type'] =  _CERBERBUS_TYPE_TO_PYTHON_TYPE[argtype]
                if kwargs['type'] == bool:
                    if kwargs.pop('default', False):
                        argname = "--no-" + name.replace("_", "-")
                        kwargs["help"] = "Disable " + name
                        kwargs['action'] = 'store_false'
                    else:
                        kwargs["help"] = "Enable " + name
                        kwargs['action'] = 'store_true'
                    del kwargs['type']

            cl_arguments[name] = ((argname, ), kwargs)

        return cl_arguments

    @property
    def resource_nodes(self) -> str:
        return "1"

    @property
    def resource_tasks_per_node(self) -> str:
        return "1"

    @property
    def resource_mail_user(self) -> str:
        return None

    @property
    def resource_mail_type(self) -> str:
        return "FAIL"

    @property
    def resource_memory(self) -> str:
        """Should return memory to be allocated in MB"""
        return None

    @property
    def resource_time(self) -> str:
        """Should return time to be allocated in seconds"""
        return None

    @property
    def resource_cpus(self) -> str:
        """Should return time to be allocated in seconds"""
        return None

    @property
    def resource_stdout_log(self) -> str:
        return self.get_output_path('.out')

    @property
    def resource_stderr_log(self) -> str:
        return self.get_output_path('.err')

    @property
    def resource_name(self) -> str:
        return '%j-{index:d}'.format_map(self.inputs)

    @property
    def resource_constraints(self) -> str:
        return None

    @property
    def resource_slurm_script(self):
        raise NotImplementedError

    def get_slurminfo_json_string(self):
        cl_opts = {
            "time" : self.resource_time,
            "job-name" : self.resource_name,
            "out" : self.resource_stdout_log,
            "err" : self.resource_stderr_log,
            "mem" : self.resource_memory,
            "cpus-per-task" : self.resource_cpus,
            "nodes" : self.resource_nodes,
            "tasks-per-node" : self.resource_tasks_per_node,
            "mail-user" : self.resource_mail_user,
            "mail-type" : self.resource_mail_type,
            "constraint" : self.resource_constraints,
            "script" : self.resource_slurm_script,
        }
        cl_opts = dict(filter(lambda kv : kv[1] is not None, cl_opts.items()))
        if "mail-user" not in cl_opts and "mail-type" in cl_opts:
            del cl_opts['mail-type']

        return json.dumps(cl_opts, indent='\t')

    @staticmethod
    def format_time(seconds : int) -> str:
        seconds = int(seconds)
        minutes = seconds//60
        seconds -= minutes*60
        hours = minutes//60
        minutes -= hours*60
        days = hours//24
        hours -= days*24
        return f'{days:d}-{hours:02d}:{minutes:02d}:{seconds:02d}'
