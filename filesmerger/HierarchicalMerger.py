
import os

from .core import merge_and_collapse_iterable
from .utils import flatten_list

class HierarchicalMerger:

    def __init__ (self, batch=1024, delimiter='\t', tmpdir=None, delete_input=False):

        assert batch>1, "HierarchicalMerger.batch must be greater than one"

        self.batch = batch
        self.delimiter = delimiter
        self.tmpdir = tmpdir
        self.delete_input = delete_input

        self._tree = [[]]
    
    def add (self, files):
        self._tree[0].extend (files)
        i=0
        while i<len(self._tree) and len(self._tree[i]) >= self.batch:
            output_path = merge_and_collapse_iterable (self._tree[i], None, self.batch, self.delimiter,
                                                       self.tmpdir, self.delete_input or i>0)
            self._tree[i] = []
            i += 1
            if i != len(self._tree):
                self._tree[i].append (output_path)
            else:
                self._tree.append ([output_path])

    def finalize (self, output_filename=None, threshold=0):

        remaining_files = flatten_list(self._tree)
        output_path = merge_and_collapse_iterable ( remaining_files, output_filename, self.batch, self.delimiter, self.tmpdir, 
                                                    self.delete_input, threshold )
        if not self.delete_input:
            # temporary files have to be deleted anyway
            tempfiles = flatten_list(self._tree[1:])
            for fname in tempfiles:
                os.remove (fname)

        self._tree = [[]]
        return output_path
    
    def generator_add_for_pipeline (self, files):
        self.add (files)
        yield [flatten_list(self._tree)]
    
     