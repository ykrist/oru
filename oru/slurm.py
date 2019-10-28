import argparse
import json
import re
import sys

class BaseSlurmInfo:
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
        raise NotImplemented


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


class SlurmArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument("--slurminfo", action='store_true')
        self.add_argument("job_index", metavar="IDX", type=int)
        self.last_args = None

    def parse_args(self, *args, **kwargs):
        ns = super().parse_args(*args, **kwargs)
        self.last_args = ns
        return ns

    def handle_slurm_info(self, slurm_info : BaseSlurmInfo):
        if self.last_args is not None and self.last_args.slurminfo:
            print(slurm_info.get_slurminfo_json_string())
            sys.exit(0)

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


