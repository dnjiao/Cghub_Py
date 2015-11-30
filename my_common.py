#!/usr/bin/env python

from tempfile import mkstemp
from shutil import move
import os, sys

def replace_line(file_path, pattern, new_line):
    fh, target_path = mkstemp()
    target = open(target_path, 'w')
    source = open(file_path, 'r')
    for line in source:
	if line.startswith(pattern):
	    target.write("%s\n" % new_line)
	else:
	    target.write(line)
    target.close()
    source.close()

    # Delete old file
    os.remove(file_path)

    # move new file
    move(target_path, file_path)

