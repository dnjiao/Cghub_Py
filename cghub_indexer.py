#/usr/bin/env python

# This script index all the downloaded data in folders named with barcode.
# It generates a .info file in each directory which has the cgquery information stored

import os,  argparse
from cghub_dnld import cgquery_check

def indexer(path):
  # Indexes all subdirectories in the current path
    for root, dirs, files in os.walk(path):
   	for d in dirs:
	    if len(d) > 15:
	    	if cgquery_check('b', d):
		    print d;
		    if root != path:
			os.system("mv %s.info %s" % (d, root))
 

def main():
    cwd = os.getcwd()
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='path', help='Path to all the directories that need to be indexed', default=cwd, type=str)
    args = parser.parse_args()
    indexer(args.path)

if __name__ == "__main__":
   main()
