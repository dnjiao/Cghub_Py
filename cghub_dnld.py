import os, sys, subprocess, argparse
import signal
from my_common import replace_line
from update_log import *

class cgRecord:
    analysis = ''
    barcode = ''
    files = []
    sums = []

MASTER = '/rsrch1/rists/cghub/RECORDS.LOG'

def cgdownload(analysis, children, norecord, *icgc_url):
# download either a list of samples in a file or single sample
    cwd = os.getcwd()
    file = os.path.join(cwd, analysis)
    print file
    print icgc_url
    rec_dict = log2dict(MASTER)
    if os.path.isfile(file):
   	print "file ", file
	print "children ", children
        download_file(file, children, rec_dict, norecord, *icgc_url)
    else:
	print "analysis ", analysis
   	print "children ", children
        download_single(cwd, analysis, children, rec_dict, norecord, *icgc_url)

def download_single(path, analysis, children, rec_dict, norecord, *icgc_url):
# Downloads single sample by analysis_id

    # If sample is downloadable
    if cgquery_check(analysis, *icgc_url):
      	record = get_info("%s/%s.info" % (path, analysis))[0]
	dir = os.path.join(path, analysis)
	filename = record.files[0]
	checksum = record.sums[0]
        # delete unfinished download dirs from previous run
	partial = os.path.join(path, analysis + ".partial")
	os.system("rm -fr %s" % partial)
	# if the analysis_id exists in master log, create softlink to downloaded files
        if analysis in rec_dict.keys():
	    print analysis + ' exists, ' + rec_dict[analysis][0]
            source_file = rec_dict[analysis][0]
	    source_dir = os.path.abspath(source_file)
            dest_file = os.path.join(dir, filename)
	    dest_dir = os.path.abspath(dest_file)
	    # File to be downloaded is not the same as the one in dict
	    # Nothing will be done if they are the same
	    if source_file != dest_file:
		# File checksum same, create link
		if checksum == rec_dict[analysis][1]:
		    if os.path.exists(dest_file):
		  	os.system("rm -f %s/*" % dest_dir)
		    else:
			os.system("mkdir -p %s" % dest_dir)
	            os.system("ln -s %s %s" % (source_file, dest_file))
		# Checksum different
		else:
		    # If file downloaded, delete older file and create link pointing to newer one
		    if os.path.exists(dest_file):
                        # Compare creation time of files
                        if os.path.getctime(source_file) < os.path.getctime(dest_file):
                            os.system("rm %s/*" % dest_dir)
                            os.system("ln -s %s %s" % (source_file, dest_file))
			    print dest_file + ' deleted and link for ' + source_file + " created"
                        else:
			    os.system("rm %s/*" % source_dir)
			    os.system("ln -s %s %s" % (dest_file, source_file))
			    print source_file + ' deleted and link for ' + dest_file + ' created'
                            rec_dict[analysis][0] = dest_file
                            rec_dict[analysis][1] = checksum
			    if norecord == 0:
			    	new_line = analysis + ' ' + dest_file + ' ' + checksum
			    	# replace old line for analysis_id in master log
			    	print "Updating master log.."
			    	replace_line(MASTER, analysis, new_line)
	

	    os.system("rm %s.info" % analysis)
	    
        # Download starts if files not existed.
	else:
	    rec_dict.setdefault(analysis, [])
	    new_line = analysis
	    #if dir exists but file does not, delete dir
   	    if os.path.isdir(dir) and not os.path.exists(os.path.join(dir, filename)):
		os.system("rm -fr %s" % dir)
	    if gtdownload(analysis, children, *icgc_url) == 1:
		print analysis + " downloaded successfully."
		# update record dictory and master log
		rec_dict[analysis].append(os.path.join(dir, filename))
            	rec_dict[analysis].append(checksum)
		if norecord == 0:
                    new_line = analysis + ' ' + os.path.join(dir, filename) + ' ' + checksum
                    log = open(MASTER, 'a')
                    log.write("%s\n" % new_line)
                    log.close()
		os.system("mv %s.info %s/" % (analysis, analysis))
	    else:
		print "Failed to download " + analysis
		os.system("rm -f %s.info" % analysis)
	    os.system("rm %s.gto" % analysis)

    # If sample is not downloadable
    else:
        print "Unable to download " + analysis
	print "If you want to read from a file, please make sure the file exists!"

    
def download_file(file, children, rec_dict, norecord, *icgc_url):
# Downloads a list of samples with analysis_id file
    f = open(file, 'r')
    lines = f.readlines()
    path = os.getcwd()
    if len(icgc_url) > 0:
	if not icgc_url[0].startswith('http'):
	    icgc_path = os.path.join(path, icgc_url[0])
	    if os.path.exists(icgc_path):
		icgc_file = open (icgc_path, 'r')
		icgc_lines = icgc_file.readlines()
		if len(lines) != len(icgc_lines):
		    sys.exit("ERROR: icgc file does not have same number of lines as analysis_id file")
	    else:
		sys.exit("ERROR: Please provide either a url or a file for ICGC downloads")
    for i, line in enumerate(lines):
	analysis = line.rstrip()
	if len(icgc_url) > 0:
	    if icgc_url[0].startswith('http'):
		download_single(path, analysis, children, rec_dict, norecord, icgc_url[0])
	    else:
	    	link = icgc_lines(i).rstrip()
	    	download_single(path, analysis, children, rec_dict, norecord, link)
	else:
	    download_single(path, analysis, children, rec_dict, norecord)

def cgquery_check(analysis, *icgc_url):
# check with cgquery to see if the sample with analysis id is available to download from cghub
    if len(icgc_url) > 0:
	query_cmd = 'cgquery -s ' + icgc_url[0] + ' analysis_id=' + analysis
    else:
    	query_cmd = "cgquery analysis_id=" + analysis
    query_pipe = subprocess.Popen(query_cmd, stdout=subprocess.PIPE, shell=True)
    print "cgquery started."
    out,err = query_pipe.communicate()
    query_pipe.wait()
    print "returncode ", query_pipe.returncode
    print out
    name = analysis + '.info'
    file = open(name, 'w')
    for row in out.splitlines():
	file.write(row + "\n")
	if 'Failed to query server version' in row:
	    print row
	    print icgc_url + ' is not a correct link'
	    os.system("rm %s" % name)
	    return False
	if 'Matching Objects                 : 0' in row:
	    print row
	    print analysis + ' does not exist'
	    file.close()
	    os.system("rm %s" % name)
	    return False
        if 'None of the matching objects are in a downloadable state' in row:
	    print row
	    print analysis + ' not downloadable'
	    file.close()
            os.system("rm %s" % name)
	    return False
    file.close()
    print "cgquery finished"
    return True

def get_info(info_file):
# Reads in and parses the query info for a sample and save it as a cgRecord object 
    infile = open(info_file, 'r')
    lines = infile.readlines()
    record_list = []
    files = []
    sums = []
    for line in lines:
	if 'analysis_id' in line:
	    analysis = line.split('                  : ')[1].rstrip()
 	if 'filename' in line:
	    files.append(line.split('             : ')[1].rstrip())
	if 'checksum' in line:
	    sums.append(line.split('             : ')[1].rstrip())
	if 'legacy_sample_id' in line:
	    barcode = line.split('             : ')[1].rstrip()
	if 'disease_abbr' in line:
	    record = cgRecord()
	    record.analysis = analysis
	    record.barcode = barcode
	    record.files = files
	    record.sums = sums
	    record_list.append(record)
	    print analysis
	    files = []
 	    sums = []
	    
    return record_list


def gtdownload(id, children, *icgc_url):
    success = 0
    if len(icgc_url) > 0:
	link = icgc_url[0] + "/cghub/data/analysis/download/" + id
	key = "/rsrch1/rists/djiao/.icgc.key"
    else:
    	link = "https://cghub.ucsc.edu/cghub/data/analysis/download/" + id
	key = "/rsrch1/rists/djiao/.cghub.key"
    cmd = "gtdownload -c " + key +" -k 15 --max-children " + str(children) + " -vv -d " + link + " 2>gt_" + id + ".log"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, preexec_fn=os.setsid)
    out,err = process.communicate() 
    process.wait()
    for row in out.splitlines(): 
	if 'Downloaded' in row:
	    success = 1    
	    break
    return success 

def main():
    parser = argparse.ArgumentParser(description='Cghub download tool')
    parser.add_argument('-c', dest='children', help='gtdownload max_children', default=4, type=int)
    parser.add_argument('-a', dest='analysis', help='anlaysis ID', required=True)
    parser.add_argument('-l', dest='url', help='GNOS endpoint')
    parser.add_argument('--norecord', action='store_true', help='download without updating master log')
    args = parser.parse_args()
    if args.norecord:
	if args.url == None:
	    cgdownload(args.analysis, args.children, 1)
	else:
	    cgdownload(args.analysis, args.children, 1, args.url)
    else:
	if args.url == None:
	    cgdownload(args.analysis, args.children, 0)
	else:
	    cgdownload(args.analysis, args.children, 0, args.url)
#    cgdownload(args.analysis, args.children, args.disease)
#    parser.add_argument('-t' dest='threads', help='threads of downloads', default=1, type=int)
 
#    group = parser.add_mutually_exclusive_group(required=True)
#    group.add_argument('-a', dest='analysis', help='analysis ID')
#    group.add_argument('-b', dest='barcode', help='barcode')       

#    args = parser.parse_args()
#    if args.analysis is not None:
#        cgdownload("a", args.analysis, args.children)
#    if args.barcode is not None:
#        cgdownload("b", args.barcode, args.children)
 
if __name__ == "__main__":
   main()

