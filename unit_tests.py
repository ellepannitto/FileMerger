import unittest
import gzip
import os
import shutil

from pprint import pprint

from filesmerger import merge_and_collapse_pattern, HierarchicalMerger

class TestFileMerger (unittest.TestCase):
    
    def _check_expected_values ( self, fin, version='n' ):
        map_versions = {
            'n': {"a": 33, "b": 16, "c":40, "d":19, "e":8, "f":10, "g":9},
            'd': {"a": 66, "b": 32, "c":80, "d":38, "e":16, "f":20, "g":18},
            't': {"a": 33, "b": 16, "c":40, "d":19},
        }
        expected_values = map_versions[version]
        for line in fin:
            line = line.strip().split()
            key, val = line[0], float(line[1])
            self.assertEqual (val, expected_values[key], "value for {} is {}. should be {}".format(key, val, expected_values[key]))
    
    def test_merge_and_collapse (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output")
        self.assertTrue (os.path.isfile("tmp_output"), "output file tmp_output does not exist")
        with gzip.open ("tmp_output", "rt") as fin:
            self._check_expected_values (fin)    
        os.remove("tmp_output")
        #check that input files were not removed
        self.assertTrue (os.path.isfile("data/a.txt"), "input file was removed")
        self.assertTrue (os.path.isfile("data/b.txt"), "input file was removed")
        self.assertTrue (os.path.isfile("data/c.txt"), "input file was removed")

    def test_merge_and_collapse_gz_output (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output.gz", "output file tmp_output.gz does not exist")
        self.assertTrue (os.path.isfile("tmp_output.gz"))
        with gzip.open ("tmp_output.gz", "rt") as fin:
            self._check_expected_values (fin)    
        os.remove("tmp_output.gz")
    
    def test_automatic_output_file_naming (self):
        output_path = merge_and_collapse_pattern ("data/*.txt")
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin)
        os.remove(output_path)

    def test_merge_and_collapse_gz_input_and_output ( self ):
        merge_and_collapse_pattern ("data/*.gz", "tmp_output2.gz")
        self.assertTrue (os.path.isfile("tmp_output2.gz"), "output file tmp_output2.gz does not exist")
        with gzip.open ("tmp_output2.gz", "rt") as fin:
            self._check_expected_values (fin)    
        os.remove("tmp_output2.gz")
        #check that input files were not removed
        self.assertTrue (os.path.isfile("data/a.gz"), "input file was removed")
        self.assertTrue (os.path.isfile("data/b.gz"), "input file was removed")
        self.assertTrue (os.path.isfile("data/c.gz"), "input file was removed")
    
    def test_merge_and_collapse_gz_input_automatic_output_file_naming ( self ):
        output_path = merge_and_collapse_pattern ("data/*.gz")
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin)    
        os.remove(output_path)

    def test_input_removing (self):
        # copy the input data into another directory
        shutil.copytree("data", "test_data")
        output_path = merge_and_collapse_pattern ("test_data/*.gz", delete_input=True)
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin)    
        os.remove(output_path)
        #check that input files were removed
        self.assertFalse (os.path.exists("test_data/a.gz"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/b.gz"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/c.gz"), "input file was not removed")
        shutil.rmtree("test_data")
    
    def test_merge_and_collapse_threshold (self):
        merge_and_collapse_pattern ("data/*.txt", "tmp_output", threshold=16)
        with gzip.open ("tmp_output", "rt") as fin:
            self._check_expected_values (fin, 't')    
        os.remove("tmp_output")
    
    def test_different_tmp_dir (self):
        os.makedirs ("./test/different/tempdir")
        output_path = merge_and_collapse_pattern ("data/*.txt", batch=2, tmpdir="./test/different/tempdir")
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin)
        os.remove (output_path)
        shutil.rmtree("./test")

    def test_merge_and_collapse_mix_txt_and_gz (self):
        merge_and_collapse_pattern ("data/*", "tmp_output")
        with gzip.open ("tmp_output", "rt") as fin:
            self._check_expected_values (fin, 'd')    
        os.remove("tmp_output")
    
    def test_hierarchical_merger (self):
        hm = HierarchicalMerger (batch=2)
        batches = [["data/a.txt"], ["data/b.txt"], ["data/c.gz", "data/a.txt"], ["data/b.txt", "data/c.txt"]]

        for batch in batches:
            hm.add (batch)
        
        output_path = hm.finalize ()
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin, "d")
        os.remove (output_path)

    def test_hierarchical_merger_threshold (self):
        hm = HierarchicalMerger (batch=2)
        batches = [["data/a.txt", "data/b.txt"], ["data/c.gz"]]

        for batch in batches:
            hm.add (batch)
        
        output_path = hm.finalize ("tmp_output", threshold=16)
        self.assertTrue (os.path.isfile(output_path), "output file does not exist")
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin, "t")
        os.remove (output_path)
        
    def test_hierarchical_merger_input_removing (self):
        # copy the input data into another directory
        shutil.copytree("data", "test_data")

        hm = HierarchicalMerger (batch=2, delete_input=True)
        batches = [["test_data/a.txt", "test_data/b.txt"], ["test_data/c.txt"], ["test_data/a.gz"], ["test_data/b.gz"]]

        for batch in batches:
            hm.add (batch)
        
        output_path = hm.finalize ("tmp_output")
        self.assertTrue (os.path.isfile(output_path), "output file does not exist")
        os.remove (output_path)
        
        #check that input files were removed
        self.assertFalse (os.path.exists("test_data/a.txt"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/a.gz"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/b.txt"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/b.gz"), "input file was not removed")
        self.assertFalse (os.path.exists("test_data/c.txt"), "input file was not removed")
        shutil.rmtree("test_data")
    
    def test_hierarchical_merger_input_not_removing (self):
        # copy the input data into another directory
        shutil.copytree("data", "test_data")

        hm = HierarchicalMerger (batch=2, delete_input=False)
        batches = [["test_data/a.txt", "test_data/b.txt"], ["test_data/c.txt"], ["test_data/a.gz"], ["test_data/b.gz"]]

        for batch in batches:
            hm.add (batch)
        
        output_path = hm.finalize ("tmp_output")
        self.assertTrue (os.path.isfile(output_path), "output file does not exist")
        os.remove (output_path)
        
        #check that input files were removed
        self.assertTrue (os.path.exists("test_data/a.txt"), "input file was removed")
        self.assertTrue (os.path.exists("test_data/a.gz"), "input file was removed")
        self.assertTrue (os.path.exists("test_data/b.txt"), "input file was removed")
        self.assertTrue (os.path.exists("test_data/b.gz"), "input file was removed")
        self.assertTrue (os.path.exists("test_data/c.txt"), "input file was removed")
        shutil.rmtree("test_data")
    
    def test_hierarchical_merger_different_tmp_dir (self):
        os.makedirs ("./test/different/tempdir")
        hm = HierarchicalMerger (batch=2, tmpdir="./test/different/tempdir")
        batches = [["data/a.txt"], ["data/b.txt"], ["data/c.gz", "data/a.txt"], ["data/b.txt", "data/c.txt"]]

        for batch in batches:
            hm.add (batch)
        
        output_path = hm.finalize ()
        with gzip.open (output_path, "rt") as fin:
            self._check_expected_values (fin, "d")
        os.remove (output_path)
        shutil.rmtree("./test")


if __name__ == "__main__":
    unittest.main ()
