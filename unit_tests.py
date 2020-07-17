import unittest
import gzip

from filesmerger import merge_and_collapse_pattern, Mode

class TestFileMerger (unittest.TestCase):
        
    def test_merge_and_collapse (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output")
        expected_values = {"a": 33, "b": 16, "c":40, "d":19, "e":8, "f":10, "g":9}
        with open ("tmp_output") as fin:
            for line in fin:
                line = line.strip().split()
                key, val = line[0], float(line[1])
                self.assertEqual (val, expected_values[key], "value for {} is {}. should be {}".format(key, val, expected_values[key]))

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
