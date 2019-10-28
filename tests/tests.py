import unittest
import gurobi
import random
import oru
import oru.gurobi
import oru.collect
import oru.slurm
import tempfile
import os
import itertools
import textwrap
import tarfile
import time

class NamedTempfileSetup(unittest.TestCase):
    def setUp(self) -> None:
        fp = tempfile.NamedTemporaryFile(delete=False)
        self.filename = fp.name
        fp.close()


    def tearDown(self) -> None:
        if os.path.exists(self.filename):
            os.remove(self.filename)


class LPModelSetup(unittest.TestCase):
    def setUp(self) -> None:
        random.seed(1337)
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


class MIPModelSetup(LPModelSetup):
    def setUp(self) -> None:
        super().setUp()
        for idx in range(len(self.vars)):
            self.vars[idx].vtype = gurobi.GRB.BINARY
        self.model.optimize()


class MIPInformationExtractionTestCase(MIPModelSetup):
    def test_mip_info(self):
        info = oru.gurobi.extract_run_information(self.model)
        self.assertIsInstance(info, oru.gurobi.MIPInformation)
        self._check_info_serialisation(info)

    def test_existing_file_load(self):
        info = oru.gurobi.ModelInformation.from_json('test_mip.json')
        self.assertIsInstance(info, oru.gurobi.MIPInformation)


class LPInformationExtrationTestCase(LPModelSetup):
    def test_lp_info(self):
        info = oru.gurobi.extract_run_information(self.model)
        self.assertIsInstance(info, oru.gurobi.LPInformation)
        self._check_info_serialisation(info)

    def test_existing_file_load(self):
        info = oru.gurobi.ModelInformation.from_json('test_lp.json')
        self.assertIsInstance(info, oru.gurobi.LPInformation)


class TableTestCase(unittest.TestCase):
    def test_min_col_width(self):
        table = oru.TablePrinter(["egg", "bacon", "minisoda"], min_col_width=7, sep="|", delay_header_print=True)
        line = table.format_line(1,2,3)
        self.assertEqual(line, "      1|      2|       3")

    def test_justify(self):
        table = oru.TablePrinter(["1234", "12345", "1234"], justify="^", sep="|", delay_header_print=True)
        line = table.format_line(1,1,11)
        self.assertEqual(line," 1  |  1  | 11 ")


class CollectTestCase(NamedTempfileSetup):
    def setUp(self):
        super().setUp()
        self.test_input_files = []
        self.test_output_files = ["collect_lp.csv", "collect_mip.csv"]
        self.links = []
        for f in ("test_lp.json", "test_mip.json"):
            links = [f.rstrip(".json") + str(i) + ".json" for i in range(1, 4)]

            for linkname in links:
                if not os.path.exists(linkname):
                    os.symlink(f, linkname)
                self.links.append(links)

            self.test_input_files.append([f] + links)


    def test_collect(self):
        for in_files, ref_outfile in zip(self.test_input_files, self.test_output_files):
            with self.subTest(in_files=in_files, ref_outfile=ref_outfile):
                oru.collect.collect_model_info(in_files, self.filename, strip_ext=False)
                with open(ref_outfile, 'r') as f:
                    ref_contents = f.read()
                with open(self.filename, 'r') as f:
                    contents = f.read()

                self.assertEqual(ref_contents, contents)


    def tearDown(self) -> None:
        super().tearDown()
        for linkname in itertools.chain(*self.links):
            if os.path.exists(linkname):
                os.remove(linkname)


class SlurmFuncTestCase(unittest.TestCase):
    def test_array_range(self):
        self.assertEqual(oru.slurm.array_range("0-4"), [0,1,2,3,4])
        self.assertEqual(oru.slurm.array_range("1,2,1-5:3"), [1,2,4])
        self.assertEqual(oru.slurm.array_range("1,,1-21:10"), [1,11,21])


class CSVLoggerTestCase(NamedTempfileSetup):
    def test_log_output(self):
        log = oru.CSVLog(self.filename,mode='w', index='epoch')
        log(epoch=0, foo='yes', bar=False, loss=0.9)
        log(epoch=1, foo='yes', bar=True, loss=0.8)
        log(foo='yes', loss=0.7, bar=False, epoch=2)
        log(epoch=3, foo='no', bar=False, loss=0.1)
        log.close()

        ref_contents = textwrap.dedent(
        """
        epoch,foo,bar,loss
        0,yes,False,0.9
        1,yes,True,0.8
        2,yes,False,0.7
        3,no,False,0.1
        """).lstrip('\n')

        with open(self.filename) as f:
            contents = f.read()

        self.assertEqual(contents, ref_contents)

    def test_file_modes(self):
        with self.assertRaises(FileExistsError):
            oru.CSVLog(self.filename)

        with self.assertRaises(ValueError):
            oru.CSVLog(self.filename, 'r')
            oru.CSVLog(self.filename, 'w+')
            oru.CSVLog(self.filename, 'r+')


class TakeTestCase(unittest.TestCase):
    def test_take(self):
        self.assertEqual(oru.take({1}), 1)
        self.assertEqual(oru.take([]), None)
        self.assertEqual(oru.take([0,1,2]), 0)


class NonzeroKeysTestCase(unittest.TestCase):
    def test_nonzero_key_get(self):
        d = {'a' : 0, 'b' : 1, 'c' : 1e-6}
        self.assertEqual(oru.get_keys_with_nonzero_val(d,eps=1e-9), ['b','c'])
        self.assertEqual(oru.get_keys_with_nonzero_val(d,eps=1e-4), ['b'])


class StopwatchTestCase(unittest.TestCase):
    def test_stopwatch(self):
        sw = oru.Stopwatch()
        time.sleep(.01)
        self.assertEqual(sw.time, 0)
        sw.start()
        self.assertTrue(sw.active)
        time.sleep(.01)
        a = sw.stop().time
        self.assertAlmostEqual(a,0.01, places=3)
        time.sleep(.01)
        b = sw.stop().time
        self.assertFalse(sw.active)
        self.assertEqual(a,b)
        sw.start()
        time.sleep(.01)
        c = sw.stop().time
        self.assertGreaterEqual(c,b)
        self.assertAlmostEqual(c, 0.02, places=3)

    def test_stopwatch_named_laps(self):
        sw = oru.Stopwatch().start()
        time.sleep(.01) # ACTIVE
        sw.stop('egg-time')
        sw.start()
        time.sleep(.01)  # ACTIVE
        sw.stop('bacon-time')
        sw.stop('bacon-time2')
        sw.start()
        time.sleep(.02)  # ACTIVE
        sw.stop('egg-time')
        sw.start()
        time.sleep(.01)  # ACTIVE
        sw.lap('dog')
        time.sleep(.01)  # ACTIVE
        sw.lap('lemon')
        sw.stop()
        sw.start()  # ACTIVE
        time.sleep(.03)
        sw.stop('cat')

        t = sw.time
        times = sw.times
        self.assertEqual(set(times.keys()), {"egg-time", "bacon-time", "dog", "cat", "lemon"})
        self.assertAlmostEqual(t, .09, places=2)
        self.assertAlmostEqual(times['egg-time'], .02, places=3)
        self.assertAlmostEqual(times['bacon-time'], .01, places=3)
        self.assertAlmostEqual(times['dog'], .01, places=3)
        self.assertAlmostEqual(times['dog'], .01, places=3)
        self.assertAlmostEqual(times['cat'], .03, places=3)


if __name__ == '__main__':
    unittest.main()
