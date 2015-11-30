#!/usr/bin/env python

# update all symbolic links in the directory

import os, sys
from update_log import *

MASTER = '/rsrch1/rists/pancancer/RECORDS.LOG'

def update():  
    cwd = os.getcwd()

    # Read master log into a dictionary
    dict = log2dict(MASTER)

    for root, dirs, files in os.walk(cwd):
	dir_name = os.path.basename(root)
	if dir_name in dict.keys():
	    dict_path = dict[dir_name][0]
	    if os.path.exists(dict_path):
	     	for file in files:
		    if file.endswith('.bam') or '.tar' in file:
		        file_path = os.path.join(root, file)
	    		# If a file is a link, get the read path (source)
	    	    	if os.path.islink(file_path):
			    link_path = os.path.realpath(file_path)
			    if link_path != dict_path:
				print 'Updating link for ' + file_path
				os.system("ln -s %s %s" % (dict_path, file_path)
			    else:
				print 'Link for ' + file_path + ' is correct.'
	    else:
	    # if path in master log does not exist
		print 'ERROR: path for ' + dir_name + ' is wrong in master log!'
	else:
	# if key (analysis ID) does not exist in master file
	    print 'ERROR: sample ' + dir_name + ' is not recorded in master log!'
		
def main():
    update()

if __name__ == "__main__":
    main()

