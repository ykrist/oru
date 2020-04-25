import json
import dataclasses
import lzma
from typing import TextIO, Dict


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

#
# _JSON_DEFINED_TYPES = {'tuple' : tuple}
#
# class JSONSerialisableObject:
#     """
#     A mixin class for making classes JSON-seriasable.  You must define a ``to_json_dict(self)`` method and a
#     ``from_json_dict(cls)`` class method.  Defining the ``__json_name__ : str = ...`` class attribute lets you customize the
#     name of the class when represented as JSON.
#     """
#
#     __json_name__: str = None
#
#     def __init_subclass__(cls, **kwargs):
#         super().__init_subclass__(**kwargs)
#         if cls.__json_name__ is None:
#             cls.__json_name__ = cls.__name__
#
#         elif not isinstance(cls.__json_name__, str):
#             raise TypeError(f'{cls!s}._json_name must be str not {type(cls.__json_name__)}')
#
#         if cls.__json_name__ in _JSON_DEFINED_TYPES:
#             raise ValueError(f"Name `{cls.__json_name__}` already defined by "
#                              f"`{_JSON_DEFINED_TYPES[cls.__json_name__].__name__}.`")
#
#         _JSON_DEFINED_TYPES[cls.__json_name__] = cls
#
#     def to_json_dict(self):
#         raise NotImplementedError
#
#     @classmethod
#     def from_json_dict(cls, data):
#         raise NotImplementedError
#
#
# def json_encode_default(o):
#     if issubclass(type(o), JSONSerialisableObject):
#         return {'!type': o.__json_name__, '!data': o.to_json_dict()}
#
#     raise TypeError(f"don't know how to serialise {type(o)} yet; "
#                     f"subclass {JSONSerialisableObject.__name__} or write your own `default()` function")
#
# def _json_encode_try_convert(o):
#     if isinstance(o, tuple):
#         return tuple_to_json_dict(o)
#     try:
#         return json_encode_default(o)
#     except TypeError:
#         return o
#
# def json_decode_object_hook(o):
#     if '!type' in o:
#         global _JSON_DEFINED_TYPES
#         cls = _JSON_DEFINED_TYPES[o['!type']]
#         data = o['!data']
#         return cls.from_json_dict(data)
#     return o
#
# def tuple_to_json_dict(t : tuple):
#     return {'!type' : 'tuple', '!data' : t}
#
# def tuple_from_json_dict(data):
#     return tuple(data)
#
# class JSONDictWrapper(JSONSerialisableObject):
#     __json_name__ = "dict_wrapper"
#
#     def __init__(self, d: Dict):
#         self.dict = d
#
#     def to_json_dict(self):
#         return [
#             {'key': _json_encode_try_convert(k),
#              'val': v} for k, v in self.dict.items()]
#
#     @classmethod
#     def from_json_dict(cls, data):
#         return {d['key'] : d['val'] for d in data }
#
#
# class JSONEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, tuple):
#             return tuple_to_json_dict(o)
#         elif issubclass(type(o), JSONSerialisableObject):
#             return {'!type': o.__json_name__, '!data': o.to_json_dict()}
#         raise TypeError(f"don't know how to serialise {type(o)} yet; "
#                         f"subclass {JSONSerialisableObject.__name__} or write your own `default()` function")
#
#
# def json_dumps(obj, *,
#                ensure_ascii=True,
#                check_circular=True,
#                allow_nan=True,
#                indent: str = None,
#                separators: str = None,
#                sort_keys=False) -> str:
#     return json.dumps(obj,
#                       cls=JSONEncoder,
#                       ensure_ascii=ensure_ascii,
#                       check_circular=check_circular,
#                       allow_nan=allow_nan,
#                       indent=indent,
#                       separators=separators,
#                       sort_keys=sort_keys)
#
#
# def json_dump(obj, fp: TextIO, *,
#               ensure_ascii=True,
#               check_circular=True,
#               allow_nan=True,
#               indent: str = None,
#               separators: str = None,
#               sort_keys=False):
#     return json.dump(obj, fp,
#                      cls=JSONEncoder,
#                      ensure_ascii=ensure_ascii,
#                      check_circular=check_circular,
#                      allow_nan=allow_nan,
#                      indent=indent,
#                      separators=separators,
#                      sort_keys=sort_keys)
#
#
# def json_load(fp: TextIO, *,
#               parse_float=None,
#               parse_int=None,
#               parse_constant=None):
#     return json.load(fp,
#                      parse_float=parse_float,
#                      parse_int=parse_int,
#                      parse_constant=parse_constant,
#                      object_hook=json_decode_object_hook)
#
#
# def json_loads(s: str, *,
#                parse_float=None,
#                parse_int=None,
#                parse_constant=None):
#     return json.loads(s,
#                       parse_float=parse_float,
#                       parse_int=parse_int,
#                       parse_constant=parse_constant,
#                       object_hook=json_decode_object_hook)
