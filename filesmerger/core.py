import heapq
import contextlib
import gzip
import glob
import os
import shutil
import tempfile

from enum import Enum

from filesmerger.utils import grouper


class Mode(Enum):
    txt = 1
    gzip = 2


def merge_and_collapse_iterable (files, output_filename=None, mode=Mode.txt, batch=1024):

    init_files = files

    if output_filename is None:
        _, output_filename = tempfile.mkstemp(text=True)

    openfunc = lambda fname: open(fname)
    openfunc_write = lambda fname: open(fname, "wt")
    if mode == Mode.gzip:
        openfunc = lambda fname: gzip.open(fname, "rt")
        openfunc_write = lambda fname: gzip.open(fname, "wt")

    first = True
    total_files_produced = 0
    tempfiles = []

    while first or len(files) > 1:

        first = False
        next_iterable = []

        with contextlib.ExitStack() as stack:
            for files_group in grouper(files, batch):
                files_group = [stack.enter_context(openfunc(fn)) for fn in files_group if fn is not None]
                outfile, outfile_name = tempfile.mkstemp(text=True)
                next_iterable.append (outfile_name)
                tempfiles.append (outfile_name)
                total_files_produced += 1
                with open(outfile_name, "w") as f:
                    f.writelines(heapq.merge(*files_group))
                # next_file_id += 1
                for fhandler in files_group:
                    fhandler.close()

        files = next_iterable

    for tmpfile in tempfiles[:-1]:
        os.remove(tmpfile)

    if False:
        for file in init_files:
            os.remove(file)

    collapse(tempfiles[-1], output_filename, mode=mode)

    os.remove(tempfiles[-1])
    
    return output_filename


def merge_and_collapse_pattern (filename_pattern, output_filename=None, mode=Mode.txt, batch=1024):
    files = glob.iglob(filename_pattern)
    return merge_and_collapse_iterable(files, output_filename, mode, batch)


def collapse (filename, output_filename, delimiter="\t", threshold=0, mode=Mode.txt):

    openfunc = lambda fname: open(fname)
    openfunc_write = lambda fname: open(fname, "wt")
    if mode == Mode.gzip:
        openfunc = lambda fname: gzip.open(fname, "rt")
        openfunc_write = lambda fname: gzip.open(fname, "wt")

    with openfunc(filename) as fin, openfunc_write(output_filename) as fout:

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
