#!/usr/bin/env python

# Traverse the current directory tree and add downloaded records to master log file in the top level
# R for recursively

import os, sys
from cghub_dnld import *
from my_common import replace_line

MASTER = '/rsrch1/rists/cghub/RECORDS.LOG'

def log2dict(file):
# read file to a dictionary
    d = {}
    if os.path.exists(file):
    	with open(file) as f:
	    for line in f:
	    	split = line.rstrip().split(' ')
	    	if len(split) == 3:
	    	    key = split[0]
	    	    d.setdefault(key, [])
	     	    d[key].append(split[1])
		    d[key].append(split[2])
	    	else:
		    print "Record broken for " + split[0]
    return d

def dict2log(dict, file):
# write dictionary to a new log file
    os.system("rm -f %s" % file)
    log = open(file, 'w')
    for key in dict:
	if len(dict[key]) == 2:
	    new_line = key + ' ' + dict[key][0] + ' ' + dict[key][1] + '\n'
	    log.write(new_line)
	else:
	    print 'Incomplete record for ' + key
    log.close()

def update_records():
# Read .info master file and search for existing files in the corresponding directories. 
# analysis_id and file path will be added to "dict"

    if os.path.exists(MASTER):
        print "Reading from master log file..."
        dict = log2dict(MASTER)
    else:
        dict = {}
    cwd = os.getcwd()
    for root, dirs, files in os.walk(cwd):
    	for dir in dirs:
	    print dir
 	# find all .info files in current directory
	    info_file = os.path.join(root, dir, "%s.info" % dir)
	    if os.path.exists(info_file):
	 	print info_file
	    	record_list = get_info(info_file)
	    	# search for analysis_id whose files exist and add to dict
	    	for rec in record_list:
		    id = rec.analysis
            	    filename = rec.files[0]
		    checksum = rec.sums[0]
		    print id + ' ' + filename + ' ' + checksum
		    file_path = os.path.join(root, dir, filename)
		    # if analysis_id already exists in dict
		    if id in dict:
		    	# duplicate downloads, delete current files and create link to old downloaded files.
			dest_dir = os.path.abspath(file_path)
		   	source_dir = os.path.abspath(dict[id][0])
			if os.path.exists(file_path): 
			    # Duplicate analysis_id found
			    if dict[id][0] != file_path:
				# Duplicate file with same checksum	
				if dict[id][1] == checksum:
				    print "Duplicate found at " + dict[id][0]
				    print "Deleting " + file_path + " and creating link"
				    os.system("rm %s/*" % dest_dir)
				    os.system("ln -s %s %s" % (dict[id][0], file_path))
				# checksum different
				else:
				    # Compare creation time of files
				    if os.path.getctime(dict[id][0]) < os.path.getctime(file_path):
					print "Current file is older"
					print "Deleting " + file_path + " and creating link"
					os.system("rm %s/*" % dest_dir)
					os.system("ln -s %s %s" % (dict[id][0], file_path))
				    else:
					print "Current file is newer"
					print "Updating dictionary.."
					os.system("rm %s/*" % source_dir)
					os.system("ln -s %s %s" % (file_path, dict[id][0]))
					dict[id][0] = file_path
					dict[id][1] = checksum
                           		new_line = id + ' ' + file_path + ' ' + checksum
                            		# replace old line for analysis_id in master log
                            		print "Updating master log.."
                            		replace_line(MASTER, id, new_line)

				
		    # if analysis_id does not exist in dict
		    else:
#			print file_path
		    	# if corresponding files are downloaded, add id/file pair to dict
                        if os.path.exists(file_path):
			    dict.setdefault(id, [])
			    dict[id].append(file_path)
			    dict[id].append(checksum)
			    new_line = id + ' ' + file_path + ' ' + checksum
			    log = open(MASTER, 'a')
                            # replace old line for analysis_id in master log
                            print "Updating master log.."
                            log.write("%s\n" % new_line)
			    log.close()

def main():
    update_records()	

if __name__ == "__main__":
    main()		

