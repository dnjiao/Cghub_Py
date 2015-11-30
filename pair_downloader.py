#!/usr/bin/env python

# Will download normal/tumor pairs and organize them together in one directory with the name of donor ID
# The input file will have three columns: donor ID, normal analysis ID, tumor analysis ID

import os, sys
from cghub_dnld import *

MASTER = '/rsrch1/rists/pancancer/RECORDS.LOG'

def pair_download_single(donor_id, normal_id, tumor_id, dict):
# Download normal/tumor pair and put them in one directory named after the donar_id
    print "donor_id " + donor_id
    donor_dir = os.path.join(os.getcwd(), donor_id)
    os.system("mkdir %s" % donor_dir)
    # If sample is downloadable
    for id in (normal_id, tumor_id):
    	if cgquery_check(id):
            record = cgRecord()
            record = get_info("%s.info" % id)[0]
	    filename = record.files[0]
	    checksum = record.sums[0]
            # if the analysis_id exists in master log, create softlink to downloaded files
	    if id in dict.keys():
		samp_dir = os.path.join(donor_dir, id)
		source = dict[id][0]
		source_dir = os.path.abspath(source)
		dest = os.path.join(samp_dir, filename)
	  	dest_dir = os.path.abspath(dest)
		if source != dest:
		    if checksum == dict[id][1]:
			if os.path.exists(dest):
			    os.system("rm %s/*" % dest_dir)
			else:
			    os.system("mkdir -p %s" % dest_dir)
			os.system("ln -s %s %s" % (source, dest))
		    else:
			if os.path.exists(dest):
			    if os.path.getctime(source) < os.path.getctime(dest):
				os.system("rm %s/*" % dest_dir)
				os.system("ln -s %s %s" % (source, dest))
                            	print dest + ' deleted and link for ' + source + " created"
                            else:
                            	os.system("rm %s/*" % source_dir)
                            	os.system("ln -s %s %s" % (dest, source))
                            	print source + ' deleted and link for ' + dest + ' created'
                            	dict[id][0] = dest
                            	dict[id][1] = checksum
                            	new_line = id + ' ' + dest + ' ' + checksum
                            # replace old line for analysis_id in master log
                            	print "Updating master log.."
                            	replace_line(MASTER, id, new_line)
		os.system("rm %s.info" % id)

	    # Download starts if files not existed.
	    else:
		dict.setdefault(id, [])
		samp_dir = os.path.join(donor_dir, id)
		if not os.path.exists(samp_dir):
		    gtdownload(id, 8)
		    os.system("rm %s.gto" % id)
		    os.system("mv %s.info %s/" % (id, id))
		    os.system("mv %s %s/" % (id, donor_dir))
		else:
		    if not os.path.exists(os.path.join(samp_dir, filename)):
			os.system("rm -fr %s" % samp_dir)
			gtdownload(id, 8)
			os.system("rm %s.gto" % id)
                    	os.system("mv %s.info %s/" % (id, id))
			os.system("mv %s %s/" % (id, donor_dir))

       	   	# update record dictory and master log
		dict[id].append(os.path.join(samp_dir, filename))
		dict[id].append(checksum)
		new_line = id + ' ' + os.path.join(samp_dir, filename) + ' ' + checksum
            	log = open(MASTER, 'a')
            	log.write("%s\n" % new_line)
            	log.close()
	# If sample is not downloadable
	else:
            print "Unable to download " + id

def pair_download_file(file, dict):
# Download a list of normal/tumor pairs of samples
    f = open(file, 'r')
    lines = f.readlines()
    for line in lines:
	donor_id = line.split("\t")[0]
	normal_id = line.split("\t")[1]
	tumor_id = line.split("\t")[2].rstrip()
	pair_download_single(donor_id, normal_id, tumor_id, dict)

def main():
    
    # Port master file into a dictionary
    dict = log2dict(MASTER)
    pair_download_file(sys.argv[1], dict)

if __name__ == "__main__":
    main()

