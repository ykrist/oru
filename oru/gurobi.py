import dataclasses
import json
import dacite

from typing import ClassVar, Dict, Any, Tuple, Union
from .constants import *
from .core import take
from gurobi import *

VarDict = Dict[Union[int, Tuple[int,...]], Var]

@dataclasses.dataclass
class ModelInformation:
    model_type : ClassVar[str] = ''
    barr_iters : int
    is_multiobj : bool
    max_coeff : float
    max_obj_coeff : float
    max_q_obj_coeff : float
    max_qc_coeff : float
    max_qc_lin_coeff : float
    max_qc_rhs : float
    max_rhs : float
    max_var_bnd : float
    min_coeff : float
    min_obj_coeff : float
    min_q_obj_coeff : float
    min_qc_coeff : float
    min_qc_lin_coeff : float
    min_qc_rhs : float
    min_rhs : float
    min_var_bnd : float
    model_name : str
    model_sense : int
    model_time : float
    num_bin_vars : int
    num_constr : int
    num_gen_constr : int
    num_int_vars : int
    num_nodes : int
    num_nz : int
    num_pwl_vars : int
    num_quad_coeff_nz : int
    num_quad_constr : int
    num_quad_nz : int
    num_sos : int
    num_start : int
    num_vars : int
    obj_bnd : float
    obj_const : float
    obj_val : float
    simplex_iters : int
    sol_count : int
    status : int

    def to_json(self, filename):
        data = dataclasses.asdict(self)
        data['model_type'] = self.model_type
        with open(filename, 'w') as f:
            json.dump(data, f, indent='\t')

    @staticmethod
    def from_json(filename):
        with open(filename, 'r') as f:
            data = json.load(f)

        mtype = data.pop('model_type')
        if mtype == 'MIP':
            cls = MIPInformation
        elif mtype in ('QP', 'QCP'):
            raise NotImplementedError
        else:
            cls = LPInformation

        return dacite.from_dict(cls, data)


@dataclasses.dataclass
class MIPInformation(ModelInformation):
    model_type = 'MIP'
    mip_gap : float
    obj_bnd_raw : float
    pool_obj_bnd : float
    pool_obj_val : float

@dataclasses.dataclass
class LPInformation(ModelInformation):
    model_type = 'LP'
    kappa : float


def extract_information(model: Model):
    if model.IsMIP == 1:
        info_class = MIPInformation
    elif model.IsQP == 1:
        raise NotImplementedError
    elif model.IsQCP == 1:
        raise NotImplementedError
    else:
        info_class = LPInformation

    kwargs = {}
    for attr in dataclasses.fields(info_class):
        kwargs[attr.name] = attr.type(model.getAttr(INFO_ATTR_TO_MODEL_ATTR[attr.name]))

    return info_class(**kwargs)


def pprint_constraint(cons : Constr, model : Model, eps=EPS):
    lhs = ''
    for var in model.getVars():
        a = model.getCoeff(cons, var)
        if abs(a) > eps:
            if a < 0:
                sgn = '-'
            else:
                sgn = '+'
            a = abs(a)
            if abs(a - 1) < eps:
                a = ''
            else:
                a = f'{a:.4g}'
            lhs += f' {sgn} {a}{var.VarName}'

    print(' '.join([lhs.rstrip().rstrip('+ '), cons.sense, str(cons.RHS)]))

#TODO : switch to a wrapper class, much easier to fiddle with than subclassing
class BaseGurobiModel(Model):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cons: Dict[str, Dict[Any, Constr]] = dict()
        self.__intvars__ = None
        self.__binvars__ = None

    @property
    def __vardicts__(self) -> Tuple[str]:
        raise NotImplementedError

def _determine_variable_types(model : BaseGurobiModel):
    intvars = []
    binvars = []
    for vargroup in model.__vardicts__:
        sample_var = take(getattr(model,'_'+vargroup).values())
        if sample_var.vtype == GRB.BINARY:
            binvars.append(vargroup)
        elif sample_var.vtype == GRB.INTEGER:
            intvars.append(vargroup)

    model.__intvars__ = tuple(intvars)
    model.__binvars__ = tuple(binvars)


def set_variables_continuous(model : BaseGurobiModel):
    if model.__intvars__ is None or model.__binvars__ is None:
        _determine_variable_types(model)
    for vargroup in model.__intvars__ + model.__binvars__:
        for var in getattr(model,'_'+vargroup).values():
            var.vtype = GRB.CONTINUOUS


def set_variables_integer(model : BaseGurobiModel):
    if model.__intvars__ is None or model.__binvars__ is None:
        return

    for vargroup in model.__intvars__:
        for var in getattr(model,'_'+vargroup).values():
            var.vtype = GRB.INTEGER

    for vargroup in model.__binvars__:
        for var in getattr(model,'_'+vargroup).values():
            var.vtype = GRB.BINARY

def get_iis_constraints(model : BaseGurobiModel):
    iis_keys = dict()
    for name, constrdict in model._cons.items():
        keys = [k for k,cons in constrdict.items() if cons.IISConstr > 0]
        if len(keys) > 0:
            iis_keys[name] = keys

    return iis_keys


def update_var_values(model : BaseGurobiModel, where=None, eps = EPS):
    for vargroup in model.__vardicts__:
        vardict_attr = '_' + vargroup
        valdict_attr = vardict_attr + 'v'
        if where is None:
            setattr(model, valdict_attr,
                    {k : var.X for k,var in getattr(model, vardict_attr).items() if var.X > eps})
        elif where == GRB.Callback.MIPSOL:
            vardict =  getattr(model, vardict_attr)
            setattr(model, valdict_attr,
                    dict((k,v) for k,v in zip(vardict.keys(), model.cbGetSolution(vardict.values())) if v > eps))
        elif where == GRB.Callback.MIPNODE:
            vardict =  getattr(model, vardict_attr)
            setattr(model, valdict_attr,
                    dict((k,v) for k,v in zip(vardict.keys(), model.cbGetNodeRel(vardict.values())) if v > eps))
        else:
            raise ValueError("`where` is must be one of: None, GRB.Callback.MIPSOL, GRB.Callback.MIPNODE")

