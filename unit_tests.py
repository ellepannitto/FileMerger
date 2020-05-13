
from filemerger.core import merge, collapse, Mode

merge ("data/*.txt", "tmp_output1.gz")
collapse ("tmp_output", "output1")

merge ("data/*.gz", "tmp_output2.gz", mode=Mode.gzip)
collapse ("tmp_output2.gz", "output2")