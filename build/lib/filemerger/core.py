import heapq
import contextlib
import gzip
import glob
import os
import shutil
import tempfile

from .utils import grouper, open_file_by_extension
import logging

logger = logging.getLogger(__name__)


def merge_and_collapse_iterable(files, output_filename=None, batch=1024, delimiter="\t",
                                tmpdir=None, delete_input=False, threshold=0):

    input_files = []
    if delete_input:
        input_files = list(files)
        files = input_files

    if output_filename is None:
        _, output_filename = tempfile.mkstemp(suffix=".gz", text=False, dir=tmpdir)

    try:
        logger.debug("merging {} files into {}".format(len(files), output_filename))
    except TypeError:
        logger.debug("merging an unknown number of files into {}".format(output_filename))

    first = True
    tempfiles = []

    while first or len(files) > 1:
        logger.debug("next iteration of hierarchical merge")

        first = False
        next_iterable = []

        with contextlib.ExitStack() as stack:
            for files_group in grouper(files, batch):
                files_group = [stack.enter_context(open_file_by_extension(fn)) for fn in files_group if fn is not None]
                _, tmpfilename = tempfile.mkstemp(text=True, suffix=".gz", dir=tmpdir)
                next_iterable.append(tmpfilename)
                tempfiles.append(tmpfilename)
                with gzip.open(tmpfilename, "wt") as f:
                    f.writelines(collapse_lines(heapq.merge(*files_group), delimiter, threshold))
                for fhandler in files_group:
                    fhandler.close()

        files = next_iterable

    logger.debug("finished merging")
    for tmpfilename in tempfiles[:-1]:
        os.remove(tmpfilename)

    if delete_input:
        for fname in input_files:
            os.remove(fname)

    shutil.move(tempfiles[-1], output_filename)

    return output_filename


def merge_and_collapse_pattern(filename_pattern, output_filename=None, batch=1024, delimiter="\t",
                               tmpdir=None, delete_input=False, threshold=0):
    files = glob.iglob(filename_pattern)
    return merge_and_collapse_iterable(files, output_filename, batch, delimiter, tmpdir, delete_input, threshold)


def collapse_lines(lines, delimiter="\t", threshold=0, check_if_input_is_sorted=True):

    try:
        firstline, firstfreq = next(lines).strip().split(delimiter)
        firstfreq = float(firstfreq)

        for lineno, line in enumerate(lines):

            el, freq = line.strip().split(delimiter)
            freq = float(freq)

            if check_if_input_is_sorted:
                assert el >= firstline, "Collapse input is not sorted at line {}: {} {}".format(lineno, firstline, el)

            if el == firstline:
                firstfreq += freq
                
            else:
                if firstfreq > threshold:
                    yield "{}{}{}\n".format(firstline, delimiter, firstfreq)
                firstline = el
                firstfreq = freq

        if firstfreq > threshold:
            yield "{}{}{}\n".format(firstline, delimiter, firstfreq)
    
    except StopIteration:
        pass


def collapse(filename, output_filename, delimiter="\t", threshold=0):

    with open_file_by_extension(filename, "rt") as fin, open_file_by_extension(output_filename, "wt") as fout:
        fout.writelines(collapse_lines(fin, delimiter, threshold))
