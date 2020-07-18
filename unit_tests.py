import unittest
import gzip
import os

from filesmerger import merge_and_collapse_pattern

class TestFileMerger (unittest.TestCase):
    
    def _check_expected_values ( self, fin ):
        expected_values = {"a": 33, "b": 16, "c":40, "d":19, "e":8, "f":10, "g":9}
        for line in fin:
            line = line.strip().split()
            key, val = line[0], float(line[1])
            self.assertEqual (val, expected_values[key], "value for {} is {}. should be {}".format(key, val, expected_values[key]))

    def test_merge_and_collapse (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output")
        self.assertTrue (os.path.isfile("tmp_output"))
        with open ("tmp_output") as fin:
            self._check_expected_values (fin)    
        os.remove("tmp_output")

    def test_merge_and_collapse_gz_output (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output.gz")
        self.assertTrue (os.path.isfile("tmp_output.gz"))
        with gzip.open ("tmp_output.gz", "rt") as fin:
            self._check_expected_values (fin)    
        os.remove("tmp_output.gz")
    
    def test_automatic_output_file_naming (self):
        output_path = merge_and_collapse_pattern ("data/*.txt")
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin)
        os.remove(output_path)

    # def test_merge_and_collapse_gz ( self ):
    #     merge_and_collapse_pattern ("data/*.gz", "tmp_output2.gz", mode=Mode.gzip)
    #     expected_values = {"a": 33, "b": 16, "c":40, "d":19, "e":8, "f":10, "g":9}
    #     with gzip.open ("tmp_output2.gz") as fin:
    #         for line in fin:
    #             line = line.strip().split()
    #             key, val = line[0], float(line[1])
    #             self.assertEqual (val, expected_values[key], "value for {} is {}. should be {}".format(key, val, expected_values[key]))

if __name__ == "__main__":
    unittest.main ()
