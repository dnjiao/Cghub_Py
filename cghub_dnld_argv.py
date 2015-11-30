#/usr/bin/env python

import os, sys, subprocess
from multiprocessing import Pool

def cghub_dnld_file(file1, file2, file3, np):
    
    # This function reads in three files with lists of analysis_id, file_name, and sample_id (barcode)
    # analysis_id is used for download link, filename is the actual name for the downloaded file and sample_id is what the file will be renamed to
    # np is the number of threads

    id_file = open(file1, 'r')
    id_lines = id_file.readlines()
    name_file = open(file2, 'r')
    name_lines = name_file.readlines()
    bar_file = open(file3, 'r')
    bar_lines = bar_file.readlines()
    
    p = Pool(int(np))
    map_args = [(line.rstrip(),name_lines[i].rstrip(),bar_lines[i].rstrip()) for i, line in enumerate(id_lines)]
    p.map(download_wrapper,map_args)
    p.close()
    p.join()

def download(id, name, bar):
    cwd = os.getcwd()
    id_dir = os.path.join(cwd,id)
    bar_dir = os.path.join(cwd,bar)
    partial = os.path.join(cwd, id + ".partial")
    
    if os.path.isdir(id_dir) and file_check(id_dir):
	dir_match_file(id_dir, id, bar)
    elif os.path.isdir(bar_dir) and file_check(bar_dir):
	dir_match_file(bar_dir, id, bar)
    else:
	if os.path.isdir(partial):
	    os.system('rm -fr %s' % partial)
        if cgquery_check(id):
	    link = "https://cghub.ucsc.edu/cghub/data/analysis/download/" + id
    	    dnld_cmd = "gtdownload -c ~/.cghub.key --max-children 4 -vv -d " + link + " > gt.out 2>gt.err"
	    subprocess.call(dnld_cmd,shell=True)
	    fname2sampid(cwd,id,name,bar)
	else:
	    print id + " does not exist"


def download_wrapper(args):
    return download(*args)

def file_check(path):
    #check if there is .bam and .bam.bai in the path
    count = 0
    bam = 0
    bai = 0
    for f in os.listdir(path):
	file = os.path.join(path, f)
	if os.path.isfile(file):
	    count = count + 1
	    if file.endswith('.bam'):
		bam = bam + 1
	    if file.endswith('.bam.bai'):
		bai = bai + 1
    if count == 2 and bam == 1 and bai == 1:
	return 1
    else:
	return 0


def cgquery_check(id):
    query_cmd = "cgquery analysis_id=" + id
    query_pipe = subprocess.Popen(query_cmd, stdout=subprocess.PIPE, shell=True)
    out,err=query_pipe.communicate()
    for row in out.splitlines():
        if 'None of the matching objects are in a downloadable state' in row:
	    print id + " no"
	    return 0
    return 1


def dir_match_file(path, id, bar):
    dir = os.path.basename(path)
    print dir, " ", id, " ", bar
    if dir == id:
	newpath = os.path.join(os.path.dirname(path), bar)
	if os.path.exists(newpath):
	    os.system('rm -fr %s' % path)
	else:
	    if os.path.exists(path):
	    	os.rename(path, newpath)
		path = newpath
	    else:
		print "Path " + path + " does not exist"
		return
    # See if name of directory matches the basename of the fiiles inside it. If not rename the files
    for file in os.listdir(path):
	basename = file.split('.bam')[0]
	if(bar != basename):
	    if file.endswith(".bam"):
	    	newname = dir + ".bam"
	    if file.endswith(".bam.bai"):
	    	newname = dir + ".bam.bai"
	    if os.path.exists(os.path.join(path, file)):
	    	os.rename(os.path.join(path, file), os.path.join(path, newname))
	    else:
		print "Path " + os.path.join(path, file) + " does not exist"
		return
	
    
def fname2sampid(path,analysis_id,filename,sample_id):
    os.rename(os.path.join(path, analysis_id + ".gto"), os.path.join(path, sample_id + ".gto"))
    os.rename(os.path.join(path, analysis_id), os.path.join(path, sample_id))
    os.rename(os.path.join(path, sample_id, filename + ".bam"), os.path.join(path, sample_id, sample_id + ".bam"))
    os.rename(os.path.join(path, sample_id, filename + ".bam.bai"), os.path.join(path, sample_id, sample_id + ".bam.bai"))


def main():
    if(len(sys.argv) != 5):
	print "Usage: cghub_dnld.py [analysis_id] [filename] [sample_id] [threads]"
	sys.exit(1)
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    file3 = sys.argv[3]
    print "in main"
    if not os.path.exists(os.path.join(os.getcwd(),file1)):
	print "File " + file1 + " doesn't exist. Quiting.."
	sys.exit(0)
    if not os.path.exists(os.path.join(os.getcwd(),file2)):
        print "File " + file2 + " doesn't exist. Quiting.."
        sys.exit(0)
    if not os.path.exists(os.path.join(os.getcwd(),file3)):
        print "File " + file3 + " doesn't exist. Quiting.."
        sys.exit(0)

    threads = sys.argv[4]
    cghub_dnld_file(file1,file2,file3,threads)
    
    
if __name__ == "__main__":
   main()

