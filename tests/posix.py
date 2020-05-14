import unittest
from oru import posix
from pathlib import Path

class PosixModuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scrapfile = Path(__file__).parent/"scrap.txt"
        if self.scrapfile.exists():
            self.scrapfile.unlink()

    def tearDown(self) -> None:
        if self.scrapfile.exists():
            self.scrapfile.unlink()

    def test_outputfile_stdin(self):
        with posix.open_default_stdout("-") as f:
            f.write("hello\n")
        self.assertTrue(not Path('-').exists())

    def test_input_outputfile_file(self):
        with posix.open_default_stdout(self.scrapfile) as f:
            f.write("hello\n")

        self.assertTrue(self.scrapfile.exists(), "file not created")
        with posix.open_default_stdin(self.scrapfile) as f:
            txt = f.read().strip()
        self.assertEqual(txt, "hello")
        with posix.open_default_stdin(self.scrapfile, mode='rb') as f:
            txt = f.read()
        self.assertNotEqual(txt, "hello\n")
        self.assertEqual(txt, b"hello\n")

if __name__ == '__main__':
    unittest.main()
