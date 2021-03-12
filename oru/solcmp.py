# Utilities for writing solution-comparing scripts
from typing import Dict, Any, Union, Iterable, Tuple
from pathlib import Path
import argparse
import json

class BadTrueData(Exception):
    def __str__(self):
        return f"Unable to load `true` data"

class Err:
    ERR_TYPE = ''
    DESC = ''
    def message(self):
        return self.DESC

    def json(self):
        return {'type' : self.ERR_TYPE, 'desc' : self.DESC}


class IoErr(Err):
    ERR_TYPE = 'io'
    DESC = "Unable to read data"

    def __init__(self,  path, detail: Union[None, str, Exception]):
        super().__init__()
        self.path = str(path)
        self.detail = str(detail) if detail else None

    def message(self):
        if self.detail:
            return f'{self.DESC}: {self.path}\n  --> {self.detail}'
        else:
            return f'{self.DESC}: {self.path}'

    def json(self):
        d = super().json()
        if self.detail:
            d['detail'] = self.detail
        d['path'] = self.path
        return d

class Suboptimal(Err):
    ERR_TYPE = 'suboptimal'
    DESC = "Not solved to optimality"


class NumberMismatch(Err):
    ERR_TYPE = 'number'
    DESC = "Numeric value mismatch"

    def __init__(self, target, value, fmt=None):
        super().__init__()
        self.target = target
        self.value = value
        self.fmt = fmt or (lambda x : x)

    def message(self) -> str:
        cmp = '<' if self.target < self.value else '>'
        return f"{self.DESC}: correct = {self.fmt(self.target)} {cmp} {self.fmt(self.value)}"

    def json(self):
        return {'target': self.target, 'value': self.value, **super().json()}

def validate_one_of(arg, argname, values):
    if arg not in values:
        raise ValueError(f"`{argname}` must be one of: " + ", ".join(map(f"{v!r}" for v in values)))



class Loader:
    def load(self, path) -> (int, Any):
        raise NotImplementedError

    def load_all(self, trues : Iterable[Path], propdir : Path,
                 ignore_prop_ioerr=False,
                 true_ioerr="raise",
                 ):
        errors = {}
        cmp = {}

        validate_one_of(true_ioerr, "true_ioerr", {"ignore", "raise", "store"})

        for truefile in trues:
            try:
                idx, true = self.load(truefile)
            except NotImplementedError:
                raise
            except Exception as e:
                if true_ioerr == "store":
                    errors[None] = [IoErr(truefile, e)]
                elif true_ioerr == "raise":
                    raise BadTrueData() from e

            propfile = propdir / truefile.name

            try:
                _, prop = self.load(propfile)
            except Exception as e:
                if not ignore_prop_ioerr:
                    errors[idx] = [IoErr(propfile, e)]
            else:
                cmp[idx] = (true, prop)

        return errors, cmp


class CheckRegistry:
    def __init__(self):
        self.checks = {}
        self.default_checks = set()
        self.check_params = {}
        self.all_params = set()

    def check(self, name, default_on=True, params=None):
        """
        A ``check`` is a function that takes two inputs, a ``true`` and a ``prop`` (proposed) and returns one or more errors
        (instance of ``Err``).

        This method should be used as a decorator to register new checks.
        """
        if name in self.checks:
            raise ValueError(f"check '{name}' already defined")

        if params:
            self.check_params[name] = list(params)
            self.all_params.update(params)
        else:
            self.check_params[name] = []

        if default_on:
            self.default_checks.add(name)

        def dec(func):
            nonlocal self, name
            self.checks[name] = func
            return func

        return dec

    def run_checks(self, cmp_values: Dict[int, Tuple[Any]], errors=None, checks=None, parameters = None):
        errors = errors or {}
        parameters = parameters or {}

        if checks is None:
            enabled_checks = self.default_checks.copy()
        else:
            enabled_checks = set(checks)

        checks = [self.checks[c] for c in enabled_checks]
        check_params = [{p: parameters[p] for p in self.check_params[c]} for c in enabled_checks]

        for idx, (true, prop) in cmp_values.items():
            for check, kwargs in zip(checks, check_params):
                for err in check(true, prop, **kwargs):
                    errors.setdefault(idx, []).append(err)

        return errors


    def add_parser_arguments(self, p: argparse.ArgumentParser):
        """ Add a set of switches to the supplied argument parser for toggling all checks on/off. """
        for c in self.checks:
            verb = 'Enabled' if c in self.default_checks else 'Disabled'
            p.add_argument(f'+{c}', f'-{c}',
                           dest=c,
                           action=NegateAction,
                           default=c in self.default_checks,
                           nargs=0,
                           help=f'Enable/Disable `{c}` checks. {verb} by default.')


    def get_enabled_checks(self, args: argparse.Namespace):
        """ Build a set of enabled checks from the supplied CL arguments.  Assumes namespace object has boolean members
        named after the checks, falling back to the default."""
        return {c for c in self.checks if getattr(args, c, c in self.default_checks)}

    def get_parameters(self, args: argparse.Namespace):
        """ Build a dict of check parameter values from the supplied CL arguments.  Assumes namespace object has members
        named after the parameters.  Will raise an exception on missing member """
        return {p : getattr(args,p) for p in self.all_params}


class NegateAction(argparse.Action):
    def __call__(self,  parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, option_string[0] == '+')

def make_default_parser():
    p = argparse.ArgumentParser(prefix_chars='-+')
    p.add_argument("true", type=Path)
    p.add_argument("prop", type=Path)
    p.add_argument('--json', action='store_true', help="Switch to JSON output")
    return p

def json_output(errors, fp):
    json_errors = []
    for idx, errs in errors.items():
        json_errors.extend({"index": idx, **e.json()} for e in errs)

    json.dump(json_errors, fp, indent='  ')

def text_output(errors, fp, idx_info=None):
    if idx_info is None:
        idx_info = lambda idx : f" INDEX {idx:>3d} ".center(100, '-')

    for idx in sorted(errors):
        errs = errors[idx]
        if len(errs) > 0:
            fp.write(idx_info(idx) + "\n")
            for e in errs:
                fp.write(e.message() + "\n")
            fp.write("\n")

