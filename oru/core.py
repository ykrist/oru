from . import constants as _C
from typing import Dict, Iterable, Callable, Any, Union
import time
import dataclasses
import json
from collections import defaultdict

def take(iterable : Iterable):
    """Pick an arbitrary element of ``iterable``."""
    for val in iterable:
        return val
    return None

def get_keys_with_nonzero_val(d : Dict, X_attr=False, eps=_C.EPS):
    if X_attr:
        getval = lambda x: x.X
    else:
        getval = lambda x: x
    return list(sorted(k for k, v in d.items() if abs(getval(v)) > eps))


class Stopwatch:
    def __init__(self):
        self._start_time = 0
        self._stop_time = 0
        self._cum_time = 0
        self._active = False

    @property
    def active(self):
        return self._active

    def start(self):
        if not self._active:
            self._active = True
            self._start_time = time.time()
        return self

    def stop(self):
        if self._active:
            self._active = False
            self._stop_time = time.time()
            self._cum_time += (self._stop_time - self._start_time)
        return self

    @property
    def time(self):
        if self._active:
            partial_time = time.time() - self._start_time
        else:
            partial_time = 0
        return partial_time + self._cum_time


@dataclasses.dataclass
class JSONSerialisableDataclass:
    def to_json_file(self, filename):
        with open(filename, 'w') as f:
            json.dump(dataclasses.asdict(self),f)

    @classmethod
    def from_json_file(cls, filename):
        with open(filename,'r') as f:
            d = json.load(f)

        return cls(**d)


@dataclasses.dataclass(eq=False, frozen=True)
class HashableFrozenDataclass:
    def __post_init__(self):
        hashval = 0
        for f in dataclasses.fields(self):
            hashval ^= hash((f.name,getattr(self, f.name)))
        self.__dict__['_hash'] = hashval # ugly hack to work around setting a frozen internal variable.

    def __hash__(self):
        return self._hash


@dataclasses.dataclass(frozen=True)
class SerialisableFrozenSlottedDataclass:
    """
    This is a workaround for allow frozen dataclasses with the __slots__ attribute to be pickled.
    See `https://stackoverflow.com/questions/55307017/pickle-a-frozen-dataclass-that-has-slots`_.
    """

    def __getstate__(self):
        return dict(
            (slot, getattr(self, slot))
            for slot in self.__slots__
            if hasattr(self, slot)
        )

    def __setstate__(self, state):
        for slot, value in state.items():
            object.__setattr__(self, slot, value)

def onerange(stop):
    return range(1,stop+1)

def group_keys_by_values(d : Dict):
    gd = defaultdict(list)
    for k,v in d.items():
        gd[v].append(k)
    for val, key_group in gd.items():
        yield key_group, val

def tuple_select_items(selection, d : Dict):
    for key,val in d.items():
        if len(key) != len(selection):
            raise ValueError(f"`selection` (len={len(selection)}) does not match length of `key`={str(key)} "
                             f"(len={len(key)})")
        for x, y in zip(selection, key):
            if x != '*' and x != y:
                break
        else:
            yield key, val

def map_keys(func : Callable[[Any], Any], d : Dict, drop_none = True) -> Dict:
    """
    Return a new dictionary from `d` by appling `func` to all keys.  If `drop_none` is True, then any keys that map to
    None are ignored.
    """
    if drop_none:
        return dict(filter(lambda kv : kv[0] is not None, zip(map(func, d.keys()), d.values())))
    else:
        return dict(zip(map(func, d.keys()), d.values()))
