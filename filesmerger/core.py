import heapq
import contextlib
import gzip
import glob
import os
import shutil
import tempfile

from enum import Enum

from filesmerger.utils import grouper, open_file_by_extension

def merge_and_collapse_iterable (files, output_filename=None, batch=1024, delimiter="\t", tmpdir=None, delete_input=False, threshold=0):

    if delete_input:
       input_files = list(files)
       files = input_files

    if output_filename is None:
        _, output_filename = tempfile.mkstemp(suffix=".gz", text=False, dir=tmpdir)

    first = True
    tempfiles = []

    while first or len(files) > 1:

        first = False
        next_iterable = []

        with contextlib.ExitStack() as stack:
            for files_group in grouper(files, batch):
                files_group = [stack.enter_context(open_file_by_extension(fn)) for fn in files_group if fn is not None]
                _, tmpfilename = tempfile.mkstemp(text=True, suffix=".gz", dir=tmpdir)
                next_iterable.append (tmpfilename)
                tempfiles.append (tmpfilename)
                with gzip.open(tmpfilename, "wt") as f:
                    f.writelines(heapq.merge(*files_group))
                for fhandler in files_group:
                    fhandler.close()

        files = next_iterable

    for tmpfilename in tempfiles[:-1]:
        os.remove(tmpfilename)

    if delete_input:
        for fname in input_files:
            os.remove(fname)

    collapse(tempfiles[-1], output_filename, delimiter, threshold)
    os.remove(tempfiles[-1])
    
    return output_filename


def merge_and_collapse_pattern (filename_pattern, output_filename=None, batch=1024, delimiter="\t", tmpdir=None, delete_input=False, threshold=0):
    files = glob.iglob(filename_pattern)
    return merge_and_collapse_iterable(files, output_filename, batch, delimiter, tmpdir, delete_input, threshold)

def collapse (filename, output_filename, delimiter="\t", threshold=0):

    with open_file_by_extension(filename, "rt") as fin, open_file_by_extension(output_filename, "wt") as fout:
        
        firstline, firstfreq = fin.readline().strip().split(delimiter)
        firstfreq = float(firstfreq)

        for line in fin:
            el, freq = line.strip().split(delimiter)
            freq = float(freq)
            if el == firstline:
                firstfreq += freq
            else:
                if firstfreq > threshold:
                    print("{}\t{}".format(firstline, firstfreq), file=fout)
                firstline = el
                firstfreq = freq

        if firstfreq > threshold:
            print("{}\t{}".format(firstline, firstfreq), file=fout)


class FileMergerForPipeline:

    def __init__(self):
        self.result_file = None

    def merge_files (self, mode, batch, filename_list):
        print("merging {} files".format(len(filename_list)))
        if self.result_file is not None:
            filename_list.append (self.result_file)
        self.result_file = merge_and_collapse_iterable(filename_list, mode=mode, batch=batch)
        print("yielding {}".format(self.result_file))
        yield [self.result_file]
