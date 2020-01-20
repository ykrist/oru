import dataclasses
import json
import dacite

from typing import ClassVar, Dict, Any, Tuple, Union, NewType, Callable
from .constants import _GUROBI_MODEL_ATTR, INFO_ATTR_TO_MODEL_ATTR, EPS
from .core import take, JSONSerialisableDataclass
from gurobi import *
from functools import wraps
from collections import deque

VarDict = Dict[Union[int, Tuple[int, ...]], Var]


class BinVarDict(VarDict):
    pass


class IntVarDict(VarDict):
    pass


class CtsVarDict(VarDict):
    pass

class CtsVar(Var):
    pass

class BinVar(Var):
    pass

class IntVar(Var):
    pass



@dataclasses.dataclass
class ModelInformation(JSONSerialisableDataclass):
    barr_iters: int
    is_multiobj: bool
    max_coeff: float
    max_obj_coeff: float
    max_q_obj_coeff: float
    max_qc_coeff: float
    max_qc_lin_coeff: float
    max_qc_rhs: float
    max_rhs: float
    max_var_bnd: float
    min_coeff: float
    min_obj_coeff: float
    min_q_obj_coeff: float
    min_qc_coeff: float
    min_qc_lin_coeff: float
    min_qc_rhs: float
    min_rhs: float
    min_var_bnd: float
    model_name: str
    model_sense: int
    model_time: float
    num_bin_vars: int
    num_constr: int
    num_gen_constr: int
    num_int_vars: int
    num_nodes: int
    num_nz: int
    num_pwl_vars: int
    num_quad_coeff_nz: int
    num_quad_constr: int
    num_quad_nz: int
    num_sos: int
    num_start: int
    num_vars: int
    obj_bnd: float
    obj_const: float
    obj_val: float
    simplex_iters: int
    sol_count: int
    status: int

    # MIP-only
    mip_gap: float
    obj_bnd_raw: float
    pool_obj_bnd: float
    pool_obj_val: float

    # LP-only
    kappa: float

def _wrap_callback(callback):
    def wrapped_callback(model : Model, where):
        return callback(model._parent, where)
    return wrapped_callback

def pprint_constraint(cons: Constr, model: Model, eps=EPS):
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


class ModelWrapper:
    model: Model

    def __init__(self, name=""):
        self.model = Model(name=name)
        self.model._parent = self

    @property
    def NumConstrs(self):
        try:
            return self.model.getAttr("NumConstrs")
        except AttributeError:
            return None

    @property
    def NumVars(self):
        try:
            return self.model.getAttr("NumVars")
        except AttributeError:
            return None

    @property
    def NumSOS(self):
        try:
            return self.model.getAttr("NumSOS")
        except AttributeError:
            return None

    @property
    def NumQConstrs(self):
        try:
            return self.model.getAttr("NumQConstrs")
        except AttributeError:
            return None

    @property
    def NumGenConstrs(self):
        try:
            return self.model.getAttr("NumGenConstrs")
        except AttributeError:
            return None

    @property
    def NumNZs(self):
        try:
            return self.model.getAttr("NumNZs")
        except AttributeError:
            return None

    @property
    def DNumNZs(self):
        try:
            return self.model.getAttr("DNumNZs")
        except AttributeError:
            return None

    @property
    def NumQNZs(self):
        try:
            return self.model.getAttr("NumQNZs")
        except AttributeError:
            return None

    @property
    def NumQCNZs(self):
        try:
            return self.model.getAttr("NumQCNZs")
        except AttributeError:
            return None

    @property
    def NumIntVars(self):
        try:
            return self.model.getAttr("NumIntVars")
        except AttributeError:
            return None

    @property
    def NumBinVars(self):
        try:
            return self.model.getAttr("NumBinVars")
        except AttributeError:
            return None

    @property
    def NumPWLObjVars(self):
        try:
            return self.model.getAttr("NumPWLObjVars")
        except AttributeError:
            return None

    @property
    def ModelName(self):
        try:
            return self.model.getAttr("ModelName")
        except AttributeError:
            return None

    @property
    def ModelSense(self):
        try:
            return self.model.getAttr("ModelSense")
        except AttributeError:
            return None

    @property
    def ObjCon(self):
        try:
            return self.model.getAttr("ObjCon")
        except AttributeError:
            return None

    @property
    def ObjVal(self):
        try:
            return self.model.getAttr("ObjVal")
        except AttributeError:
            return None

    @property
    def ObjBound(self):
        try:
            return self.model.getAttr("ObjBound")
        except AttributeError:
            return None

    @property
    def ObjBoundC(self):
        try:
            return self.model.getAttr("ObjBoundC")
        except AttributeError:
            return None

    @property
    def PoolObjBound(self):
        try:
            return self.model.getAttr("PoolObjBound")
        except AttributeError:
            return None

    @property
    def PoolObjVal(self):
        try:
            return self.model.getAttr("PoolObjVal")
        except AttributeError:
            return None

    @property
    def MIPGap(self):
        try:
            return self.model.getAttr("MIPGap")
        except AttributeError:
            return None

    @property
    def Runtime(self):
        try:
            return self.model.getAttr("Runtime")
        except AttributeError:
            return None

    @property
    def Status(self):
        try:
            return self.model.getAttr("Status")
        except AttributeError:
            return None

    @property
    def SolCount(self):
        try:
            return self.model.getAttr("SolCount")
        except AttributeError:
            return None

    @property
    def IterCount(self):
        try:
            return self.model.getAttr("IterCount")
        except AttributeError:
            return None

    @property
    def BarIterCount(self):
        try:
            return self.model.getAttr("BarIterCount")
        except AttributeError:
            return None

    @property
    def NodeCount(self):
        try:
            return self.model.getAttr("NodeCount")
        except AttributeError:
            return None

    @property
    def IsMIP(self):
        try:
            return self.model.getAttr("IsMIP")
        except AttributeError:
            return None

    @property
    def IsQP(self):
        try:
            return self.model.getAttr("IsQP")
        except AttributeError:
            return None

    @property
    def IsQCP(self):
        try:
            return self.model.getAttr("IsQCP")
        except AttributeError:
            return None

    @property
    def IsMultiObj(self):
        try:
            return self.model.getAttr("IsMultiObj")
        except AttributeError:
            return None

    @property
    def IISMinimal(self):
        try:
            return self.model.getAttr("IISMinimal")
        except AttributeError:
            return None

    @property
    def MaxCoeff(self):
        try:
            return self.model.getAttr("MaxCoeff")
        except AttributeError:
            return None

    @property
    def MinCoeff(self):
        try:
            return self.model.getAttr("MinCoeff")
        except AttributeError:
            return None

    @property
    def MaxBound(self):
        try:
            return self.model.getAttr("MaxBound")
        except AttributeError:
            return None

    @property
    def MinBound(self):
        try:
            return self.model.getAttr("MinBound")
        except AttributeError:
            return None

    @property
    def MaxObjCoeff(self):
        try:
            return self.model.getAttr("MaxObjCoeff")
        except AttributeError:
            return None

    @property
    def MinObjCoeff(self):
        try:
            return self.model.getAttr("MinObjCoeff")
        except AttributeError:
            return None

    @property
    def MaxRHS(self):
        try:
            return self.model.getAttr("MaxRHS")
        except AttributeError:
            return None

    @property
    def MinRHS(self):
        try:
            return self.model.getAttr("MinRHS")
        except AttributeError:
            return None

    @property
    def MaxQCCoeff(self):
        try:
            return self.model.getAttr("MaxQCCoeff")
        except AttributeError:
            return None

    @property
    def MinQCCoeff(self):
        try:
            return self.model.getAttr("MinQCCoeff")
        except AttributeError:
            return None

    @property
    def MaxQCLCoeff(self):
        try:
            return self.model.getAttr("MaxQCLCoeff")
        except AttributeError:
            return None

    @property
    def MinQCLCoeff(self):
        try:
            return self.model.getAttr("MinQCLCoeff")
        except AttributeError:
            return None

    @property
    def MaxQCRHS(self):
        try:
            return self.model.getAttr("MaxQCRHS")
        except AttributeError:
            return None

    @property
    def MinQCRHS(self):
        try:
            return self.model.getAttr("MinQCRHS")
        except AttributeError:
            return None

    @property
    def MaxQObjCoeff(self):
        try:
            return self.model.getAttr("MaxQObjCoeff")
        except AttributeError:
            return None

    @property
    def MinQObjCoeff(self):
        try:
            return self.model.getAttr("MinQObjCoeff")
        except AttributeError:
            return None

    @property
    def Kappa(self):
        try:
            return self.model.getAttr("Kappa")
        except AttributeError:
            return None

    @property
    def KappaExact(self):
        try:
            return self.model.getAttr("KappaExact")
        except AttributeError:
            return None

    @property
    def FarkasProof(self):
        try:
            return self.model.getAttr("FarkasProof")
        except AttributeError:
            return None

    @property
    def TuneResultCount(self):
        try:
            return self.model.getAttr("TuneResultCount")
        except AttributeError:
            return None

    @property
    def NumStart(self):
        try:
            return self.model.getAttr("NumStart")
        except AttributeError:
            return None

    @property
    def LicenseExpiration(self):
        try:
            return self.model.getAttr("LicenseExpiration")
        except AttributeError:
            return None

    @property
    def JobID(self):
        try:
            return self.model.getAttr("JobID")
        except AttributeError:
            return None

    @property
    def Server(self):
        try:
            return self.model.getAttr("Server")
        except AttributeError:
            return None

    def addConstr(self, lhs, sense=None, rhs=None, name=""):
        return self.model.addConstr(lhs, sense, rhs, name)

    def addConstrs(self, constrs, name=""):
        return self.model.addConstrs(constrs, name=name)

    def addGenConstrAbs(self, resvar, argvar, name):
        return self.model.addGenConstrAbs(resvar, argvar, name)

    def addGenConstrAnd(self, resvar, vars, name):
        return self.model.addGenConstrAnd(resvar, vars, name)

    def addGenConstrIndicator(self, binvar, binval, lhs, sense, rhs, name):
        return self.model.addGenConstrIndicator(binvar, binval, lhs, sense, rhs, name)

    def addGenConstrMax(self, resvar, vars, constant, name):
        return self.model.addGenConstrMax(resvar, vars, constant, name)

    def addGenConstrMin(self, resvar, vars, constant, name):
        return self.model.addGenConstrMin(resvar, vars, constant, name)

    def addGenConstrOr(self, resvar, vars, name):
        return self.model.addGenConstrOr(resvar, vars, name)

    def addLConstr(self, lhs, sense=None, rhs=None, name=""):
        return self.model.addLConstr(lhs, sense, rhs, name)

    def addQConstr(self, lhs, sense=None, rhs=None, name=""):
        return self.model.addQConstr(lhs, sense, rhs, name)

    def addRange(self, expr, lower, upper, name=""):
        return self.model.addRange(expr, lower, upper, name)

    def addSOS(self, type, vars, wts):
        return self.model.addSOS(type, vars, wts)

    def addVar(self, lb=0.0, ub=GRB.INFINITY, obj=0.0, vtype=GRB.CONTINUOUS, name="", column=None):
        return self.model.addVar(lb, ub, obj, vtype, name, column)

    def addVars(self, *indexes, lb=0.0, ub=None, obj=0.0, vtype=None, name=""):
        return self.model.addVars(*indexes, lb=lb, ub=ub, obj=obj, vtype=vtype, name=name)

    def cbCut(self, lhs, sense=None, rhs=None):
        return self.model.cbCut(lhs, rhs, sense)

    def cbGet(self, what):
        return self.model.cbGet(what)

    def cbGetNodeRel(self, vars):
        return self.model.cbGetNodeRel(vars)

    def cbGetSolution(self, vars):
        return self.model.cbGetSolution(vars)

    def cbLazy(self, lhs, sense=None, rhs=None):
        return self.model.cbLazy(lhs, rhs, sense)

    def cbSetSolution(self, vars, values):
        return self.model.cbSetSolution(vars, values)

    def cbStopOneMultiObj(self, objnum):
        return self.model.cbStopOneMultiObj(objnum)

    def cbUseSolution(self, *args, **kwargs):
        return self.model.cbUseSolution(*args, **kwargs)

    def chgCoeff(self, constr, var, newvalue):
        return self.model.chgCoeff(constr, var, newvalue)

    def computeIIS(self):
        return self.model.computeIIS()

    def copy(self):
        return self.model.copy()

    def discardConcurrentEnvs(self):
        return self.model.discardConcurrentEnvs()

    def discardMultiobjEnvs(self):
        return self.model.discardMultiobjEnvs()

    def display(self, *args, **kwargs):
        return self.model.display(*args, **kwargs)

    def feasibility(self):
        return self.model.feasibility()

    def feasRelax(self, relaxobjtype, minrelax, vars, lbpen, ubpen, constrs, rhspen):
        return self.model.feasRelax(relaxobjtype, minrelax, vars, lbpen, ubpen, constrs, rhspen)

    def feasRelaxS(self, relaxobjtype, minrelax, vrelax, crelax):
        return self.model.feasRelaxS(relaxobjtype, minrelax, vrelax, crelax)

    def fixed(self):
        return self.model.fixed()

    def getAttr(self, attrname):
        return self.model.getAttr(attrname)

    def getCoeff(self, constr, var):
        return self.model.getCoeff(constr, var)

    def getCol(self, var):
        return self.model.getCol(var)

    def getConcurrentEnv(self):
        return self.model.getConcurrentEnv()

    def getConstrByName(self):
        return self.model.getConstrByName()

    def getConstrs(self):
        return self.model.getConstrs()

    def getGenConstrAbs(self, genconstr):
        return self.model.getGenConstrAbs(genconstr)

    def getGenConstrAnd(self, genconstr):
        return self.model.getGenConstrAnd(genconstr)

    def getGenConstrIndicator(self, genconstr):
        return self.model.getGenConstrIndicator(genconstr)

    def getGenConstrMax(self, genconstr):
        return self.model.getGenConstrMax(genconstr)

    def getGenConstrMin(self, genconstr):
        return self.model.getGenConstrMin(genconstr)

    def getGenConstrOr(self, genconstr):
        return self.model.getGenConstrOr(genconstr)

    def getGenConstrs(self):
        return self.model.getGenConstrs()

    def getMultiobjEnv(self):
        return self.model.getMultiobjEnv()

    def getObjective(self):
        return self.model.getObjective()

    def getParamInfo(self, paramname):
        return self.model.getParamInfo(paramname)

    def getPWLObj(self, var):
        return self.model.getPWLObj(var)

    def getQConstrs(self):
        return self.model.getQConstrs()

    def getQCRow(self, qc):
        return self.model.getQCRow(qc)

    def getRow(self, constr):
        return self.model.getRow(constr)

    def getSOS(self, sos):
        return self.model.getSOS(sos)

    def getSOSs(self):
        return self.model.getSOSs()

    def getTuneResult(self):
        return self.model.getTuneResult()

    def getVarByName(self):
        return self.model.getVarByName()

    def getVars(self):
        return self.model.getVars()

    def linearize(self):
        return self.model.linearize()

    def message(self, msg):
        return self.model.message(msg)

    def optimize(self, callback=None):
        if callback is None:
            return self.model.optimize()
        else:

            return self.model.optimize(_wrap_callback(callback))

    def presolve(self):
        return self.model.presolve()

    def printAttr(self, attrname, filter):
        return self.model.printAttr(attrname, filter)

    def printQuality(self):
        return self.model.printQuality()

    def printStats(self):
        return self.model.printStats()

    def read(self, filename):
        return self.model.read(filename)

    def relax(self):
        return self.model.relax()

    def remove(self, items):
        return self.model.remove(items)

    def reset(self):
        return self.model.reset()

    def resetParams(self):
        return self.model.resetParams()

    def setAttr(self, attrname, newvalue):
        return self.model.setAttr(attrname, newvalue)

    def setObjective(self, expression, sense=None):
        return self.model.setObjective(expression, sense=sense)

    def setObjectiveN(self, expression, index):
        return self.model.setObjectiveN(expression, index)

    def setParam(self, paramname, newvalue):
        return self.model.setParam(paramname, newvalue)

    def setPWLObj(self, var, x, y):
        return self.model.setPWLObj(var, x, y)

    def terminate(self):
        return self.model.terminate()

    def tune(self):
        return self.model.tune()

    def update(self):
        self.model.update()

    def write(self, filename):
        self.model.write(filename)

class BaseGurobiModel(ModelWrapper):
    def __init__(self, name=""):
        super().__init__(name=name)
        self.__lonevars__ = []
        self.__intvars__ = []
        self.__binvars__ = []
        self.__ctsvars__ = []
        for attrname, attrtype in self.__annotations__.items():  # Black 3.7 magic
            if attrtype == BinVarDict:
                self.__binvars__.append(attrname)
            elif attrtype == BinVar:
                self.__binvars__.append(attrname)
                self.__lonevars__.append(attrname)
            elif attrtype == IntVarDict:
                self.__intvars__.append(attrname)
            elif attrtype == IntVar:
                self.__intvars__.append(attrname)
                self.__lonevars__.append(attrname)
            elif attrtype == CtsVarDict:
                self.__ctsvars__.append(attrname)
            elif attrtype == CtsVar:
                self.__ctsvars__.append(attrname)
                self.__lonevars__.append(attrname)
            elif attrtype in (VarDict,Var):
                raise TypeError("VarDict/Var should not be used to denote variables, use BinVarDict, IntVarDict or "
                                "CtsVarDict, or their *Var equivalents instead.")
            else:
                continue
            if attrname in self.__lonevars__:
                setattr(self, attrname, None)
                setattr(self, attrname + 'v', None)
            else:
                setattr(self, attrname, attrtype())
                setattr(self, attrname + 'v', dict())

        self.__intvars__ = tuple(self.__intvars__)
        self.__binvars__ = tuple(self.__binvars__)
        self.__ctsvars__ = tuple(self.__ctsvars__)
        self.__vars__ = self.__ctsvars__ + self.__binvars__ + self.__intvars__
        self.cut_cache = dict()
        self.cut_cache_size = 0
        self.cons: Dict[str, Dict[Any, Constr]] = dict()

    def set_vars_attrs(self, **kwargs):
        """Convenience function to set the variable dictionaries on a model, checking if all have been provided. """
        a = set(kwargs.keys())
        b = set(self.__vars__)
        missing = b - a
        if len(missing) > 0:
            raise ValueError("Missing variables: ", ",".join(missing))
        extra = a - b
        if len(extra) > 0:
            raise ValueError("Undefined variables: ", ",".join(extra))
        for varname, vardict in kwargs.items():
            setattr(self, varname, vardict)

    def set_variables_continuous(self):
        for var_attr in self.__intvars__ + self.__binvars__:
            if var_attr in self.__lonevars__:
                getattr(self, var_attr).vtype = GRB.CONTINUOUS
            else:
                for var in getattr(self, var_attr).values():
                    var.vtype = GRB.CONTINUOUS

    def set_variables_integer(self):
        for var_attr in self.__intvars__ + self.__binvars__:
            vtype = GRB.BINARY if var_attr in self.__binvars__ else GRB.INTEGER
            if var_attr in self.__lonevars__:
                getattr(self, var_attr).vtype = vtype
            else:
                for var in getattr(self, var_attr).values():
                    var.vtype = vtype

    def get_iis_constraints(self):
        iis_keys = dict()
        for name, constrdict in self.cons.items():
            if isinstance(constrdict, Constr):
                if constrdict.IISConstr > 0:
                    iis_keys[name] = None
            else:
                keys = [k for k, cons in constrdict.items() if cons.IISConstr > 0]
                if len(keys) > 0:
                    iis_keys[name] = keys

        return iis_keys

    def update_var_values(self, where=None, eps=EPS):
        for var_attr in self.__vars__:
            val_attr = var_attr + 'v'
            if where is None:
                if var_attr in self.__lonevars__:
                    setattr(self, val_attr, getattr(self, var_attr).X)
                else:
                    setattr(self, val_attr,
                            {k: var.X for k, var in getattr(self, var_attr).items() if var.X > eps})

            elif where == GRB.Callback.MIPSOL:
                if var_attr in self.__lonevars__:
                    setattr(self, val_attr, self.cbGetSolution(getattr(self, var_attr)))
                else:
                    vardict = getattr(self, var_attr)
                    setattr(self, val_attr,
                            dict((k, v) for k, v in zip(vardict.keys(), self.cbGetSolution(vardict.values())) if v > eps))
            elif where == GRB.Callback.MIPNODE:
                if var_attr in self.__lonevars__:
                    setattr(self, val_attr, self.cbGetNodeRel(getattr(self, var_attr)))
                else:
                    vardict = getattr(self, var_attr)
                    setattr(self, val_attr,
                            dict((k, v) for k, v in zip(vardict.keys(), self.cbGetNodeRel(vardict.values())) if v > eps))
            else:
                raise ValueError("`where` is must be one of: None, GRB.Callback.MIPSOL, GRB.Callback.MIPNODE")

    def flush_cut_cache(self):
        total =0
        for constraint_name in self.cut_cache:
            if isinstance(self.cut_cache[constraint_name], dict):
                self.cons[constraint_name] = dict()
                for idx, cut in self.cut_cache[constraint_name].items():
                    self.cons[constraint_name][idx] = self.addConstr(cut)
                    total += 1
            else:
                self.cons[constraint_name] = [self.addConstr(cut) for cut in self.cut_cache[constraint_name]]
                total += 1
        self.cut_cache.clear()
        self.cut_cache_size = 0
        return total

    def get_model_information(self) -> ModelInformation:
        kwargs = {}
        for attr in dataclasses.fields(ModelInformation):
            try:
                val = attr.type(self.model.getAttr(INFO_ATTR_TO_MODEL_ATTR[attr.name]))
            except AttributeError:
                val = None
            kwargs[attr.name] = val

        return ModelInformation(**kwargs)

    def _add_cut_to_cache(self, cut, cache, cache_key=None):
        if cache_key is not None:
            if cache not in self.cut_cache:
                self.cut_cache[cache] = dict()
            self.cut_cache[cache][cache_key] = cut
            self.cut_cache_size += 1
        else:
            if cache not in self.cut_cache:
                self.cut_cache[cache] = deque()
            self.cut_cache[cache].append(cut)
            self.cut_cache_size += 1

    def cbCut(self, cut : TempConstr, cache : str =None, cache_key=None):
        super().cbCut(cut)
        if cache is not None:
            self._add_cut_to_cache(cut, cache, cache_key)

    def cbLazy(self, cut : TempConstr, cache : str =None, cache_key=None):
        super().cbLazy(cut)
        if cache is not None:
            self._add_cut_to_cache(cut, cache, cache_key)

    def temp_params(self, **param_val_pairs):
        return TempModelParameters(self, **param_val_pairs)


class TempModelParameters:
    """
    Context manager which temporarily sets parameters and restores them after.
    """
    def __init__(self, model : BaseGurobiModel, **param_val_pairs):
        self.model = model
        self.old_parameters = dict()
        self.new_parameters = param_val_pairs
        for param,new_val in param_val_pairs.items():
            _,_, old_val,_,_,_ = model.getParamInfo(param)
            self.old_parameters[param] = old_val

    def __enter__(self):
        for param, val in self.new_parameters.items():
            self.model.setParam(param, val)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for param,val in self.old_parameters.items():
            self.model.setParam(param,val)