
import gzip

def grouper (iterable, n):
    out = []
    for x in iterable:
        out.append (x)
        if len(out) == n:
            yield out
            out = []
    if out:
        yield out

def open_file_by_extension ( filename: str, mode=None ):
    if filename.endswith(".gz"):
        if mode == None: 
            mode="rt"
        return gzip.open (filename, mode)
    else:
        if mode == None:
            mode = "r"
        return open(filename, mode)
