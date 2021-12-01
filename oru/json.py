import json
import dataclasses
import lzma
from typing import Dict, Tuple, Any
from pathlib import Path
import re
from .core import map_keys

NestedDict = Dict[str, Dict]
Key = str
TupleKey = Tuple[Key, ...]
FlatDict = Dict[TupleKey, Any]


@dataclasses.dataclass
class JSONSerialisableDataclass:
    def to_json_file(self, filename, compress=False):
        if compress:
            fp = lzma.open(filename, 'wt')
        else:
            fp = open(filename, 'w')

        json.dump(dataclasses.asdict(self), fp)
        fp.close()

    @classmethod
    def from_json_file(cls, filename):
        with open(filename, 'r') as f:
            d = json.load(f)

        return cls(**d)


def _try_load_json_file(p : Path):
    try:
        p = p.resolve()
    except ValueError as e:
        return (None, f"`{p}` doesn't look like a valid filepath: {e!s}")

    try:
        with open(p, 'r') as fp:
            return (json.load(fp), None)
    except Exception as e:
        return (None, f"Unable to read `{p}`: {e!s}")

DEFAULT_RESOLVE_PATTERN = re.compile('\.json$')

def resolve_files(val, rootpath : Path, regexp : re.Pattern = DEFAULT_RESOLVE_PATTERN, callback=None, depth=None) -> dict:
    if depth is None:
        new_depth = 0
    elif depth <= 0:
        return val
    else:
        new_depth = depth - 1

    if isinstance(val, dict):
        for k,v in val.items():
            val[k] = resolve_files(v, rootpath, regexp, callback, new_depth)

    elif isinstance(val, list):
        val = [resolve_files(v, rootpath, regexp, callback, new_depth) for v in val]

    elif isinstance(val, str) and regexp.search(val) is not None:
        newval, reason = _try_load_json_file(rootpath / val)

        if newval is not None:
            val = resolve_files(newval, rootpath, regexp, callback, new_depth)
        elif callback is not None:
            callback(rootpath / val, reason)

    return val

def flatten_dictionary(d: NestedDict, _prefix=()) -> FlatDict:
    new_d = {}
    for key in d:
        new_key = _prefix + (key,)
        if isinstance(d[key], dict) and len(d[key]) > 0:
            new_d.update(flatten_dictionary(d[key], new_key))
        else:
            new_d[new_key] = d[key]
    return new_d


def unflatten_dictionary(d: FlatDict) -> NestedDict:
    new_d = {}
    for reckey in d:
        current_d = new_d
        for key in reckey[:-1]:
            if key not in current_d:
                current_d[key] = {}
            current_d = current_d[key]
        current_d[reckey[-1]] = d[reckey]
    return new_d

def expand_tuplekeys(d: FlatDict, sep: str) -> FlatDict:
    return map_keys(lambda tuplekey: tuple(k for key in tuplekey for k in key.split(sep)), d)


def join_tuplekeys(d: FlatDict, sep: str) -> Dict[str, Any]:
    return map_keys(lambda tuplekey: sep.join(tuplekey), d)
