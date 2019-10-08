import unittest
import gurobi
import random
from pkgs.oru import oru
import tempfile
import os
random.seed(1337)



class LPSetupTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.model = gurobi.Model()
        self.model.setParam('OutputFlag', 0)
        X = [self.model.addVar(ub=1, obj=(1+random.random()*10)) for _ in range(100)]
        self.constr = []
        for _ in range(5):
            var_mask = [random.random() < 0.3 for _ in range(len(X))]
            self.constr.append(self.model.addConstr(
                gurobi.quicksum(x for x,m in zip(X,var_mask) if m) <= sum(var_mask) - 1
            ))
        self.constr.append(self.model.addConstr(gurobi.quicksum(x*random.randint(5,20) for x in X) <= 100))
        self.vars = X
        self.model.optimize()

    def _check_info_serialisation(self, info):
        fp = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        fp.close()
        info.to_json(fp.name)
        info_copy = info.__class__.from_json(fp.name)
        self.assertEqual(info, info_copy)
        os.remove(fp.name)


class MIPSetupTestCase(LPSetupTestCase):
    def setUp(self) -> None:
        super().setUp()
        for idx in range(len(self.vars)):
            self.vars[idx].vtype = gurobi.GRB.BINARY
        self.model.optimize()


class MIPInformationExtractionTestCase(MIPSetupTestCase):
    def test_mip_info(self):
        info = oru.extract_information(self.model)
        self.assertIsInstance(info, oru.MIPInformation)
        self._check_info_serialisation(info)


class LPInformationExtrationTestCase(LPSetupTestCase):
    def test_lp_info(self):
        info = oru.extract_information(self.model)
        self.assertIsInstance(info, oru.LPInformation)
        self._check_info_serialisation(info)



if __name__ == '__main__':
    unittest.main()
