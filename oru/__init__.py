import dataclasses
import gurobi
import json
from typing import ClassVar
from .constants import *
import dacite
from .table import *


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


def extract_information(model: gurobi.Model):
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
