#/usr/bin/env python

import os, sys
link = "https://cghub.ucsc.edu/cghub/data/analysis/download/" + sys.argv[1]
cmd = "gtdownload -c ~/.cghub.key --max-children 4 -vv -d " + link + " 2>gt.err"os.system(cmd)
cmd = "mv " + sys.argv[1] + " " + sys.argv[2]
os.system(cmd)

