from . import constants as _C
from typing import Dict, Iterable, Callable, Any, Union
import time
import dataclasses
import json
from collections import defaultdict
import os

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
        self._total_time = 0
        self._lap_time = 0
        self._active = False
        self._times = dict()

    @property
    def active(self):
        return self._active

    def start(self):
        if not self._active:
            self._active = True
            self._start_time = time.time()
            self._lap_time = 0
        return self

    def stop(self, label=None):
        if self._active:
            self._active = False
            self._stop_time = time.time()
            self._lap_time = self._stop_time - self._start_time
            self._total_time += self._lap_time
            if label is not None:
                self._times[label] = self._lap_time

        return self

    def lap(self, label):
        if self.active:
            self._stop_time = time.time()
            self._lap_time = self._stop_time - self._start_time
            self._total_time += self._lap_time
            self._times[label] = self._lap_time
            self._start_time = self._stop_time
        return self

    @property
    def time(self):
        if self.active:
            return self._total_time + self.lap_time
        else:
            return self._total_time

    @property
    def lap_time(self):
        if self._active:
            return time.time() - self._start_time
        else:
            return self._lap_time

    @property
    def times(self):
        return self._times

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
class LazyHashFrozenDataclass:
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

def filterl(func,iterable):
    return list(filter(func, iterable))



class SurjectiveDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mapped_keys = dict()  # ptr_key -> key
        self._mapped_keys_inv = defaultdict(set)

    # TODO add tests
    def share_value(self, new_key, existing_key):
        assert existing_key in self
        existing_key = self._mapped_keys.get(existing_key, existing_key)
        if new_key in self._mapped_keys:
            self._mapped_keys_inv[self._mapped_keys[new_key]].remove(new_key)
        elif super().__contains__(new_key):
            super().__delitem__(new_key)

        self._mapped_keys[new_key] = existing_key
        self._mapped_keys_inv[existing_key].add(new_key)

    @property
    def keymap(self):
        return self._mapped_keys.copy()

    @property
    def inverse_keymap(self):
        return dict((k,v) for k,v in self._mapped_keys_inv.items() if len(v) > 0)

    def get_one_to_one(self):
        return super().copy()

    def unique_items(self):
        return super().items()

    def unique_keys(self):
        return super().keys()

    def keys(self):
        for key in super().keys():
            yield key
            for ptr_key in self._mapped_keys_inv[key]:
                yield ptr_key

    def items(self):
        for key, val in super().items():
            yield key,val
            for ptr_key in self._mapped_keys_inv[key]:
                yield ptr_key, val

    def copy(self):
        new = SurjectiveDict(super().copy())
        new._mapped_keys = self._mapped_keys.copy()
        new._mapped_keys_inv = self._mapped_keys_inv.copy()
        return new

    def __len__(self):
        return len(self._mapped_keys) + super().__len__()

    def __getitem__(self, item):
        return super().__getitem__(self._mapped_keys.get(item, item))

    def __contains__(self, item):
        return super().__contains__(self._mapped_keys.get(item, item))

    def __setitem__(self, item, val):
        return super().__setitem__(self._mapped_keys.get(item, item), val)

    def __delitem__(self, key):
        if key in self._mapped_keys:
            del self._mapped_keys[key]
        else:
            if len(self._mapped_keys_inv[key]) > 0:
                # a new key will take over as the parent key
                new_key = self._mapped_keys_inv[key].pop()
                # update mappings
                for k in self._mapped_keys_inv[key]:
                    self._mapped_keys[k] = new_key
                self._mapped_keys_inv[new_key] = self._mapped_keys_inv[key]
                del self._mapped_keys_inv[key]
                del self._mapped_keys[new_key]
                # copy data to new parent
                super().__setitem__(new_key, self[key])
            super().__delitem__(key)

    def __repr__(self):
        return '{ ' + ', '.join(f'{repr(k)} : {repr(v)}' for k,v in self.items()) + ' }'

    def __str__(self):
        return self.__repr__()


class FrozenOrderedSet(frozenset):
    def __init__(self, seq=()):
        super().__init__(seq)
        self._order = tuple(seq)
        self._hash = None

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(self._order)
        return self._hash

    def __iter__(self):
        return iter(self._order)

    def __getitem__(self, item):
        return self._order[item]


class FileNameManager:
    def __init__(self, directory='./', prefix='', create_dirs=False):
        if create_dirs:
            os.makedirs(directory, exist_ok=True)
        self.directory = directory
        self.prefix = prefix
        self.paths = set()

    def __call__(self, suffix, prefix=True, ignore_duplicates=False):
        if prefix:
            path = os.path.join(self.directory, self.prefix + suffix)
        else:
            path = os.path.join(self.directory, suffix)
        if not ignore_duplicates and path in self.paths:
            raise Exception(f"{path} may be duplicated.")
        self.paths.add(path)
        return path
