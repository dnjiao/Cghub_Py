#!/usr/bin/env python

# find out the records which have not been downloaded based on
# 1. analysis.txt (argv[1]) and 2. subfolders in this directory

import sys, os

file = open(sys.argv[1],'r')
lines = file.readlines()

list = []
for dir in os.listdir(os.getcwd()):
    if os.path.isdir(dir):
	list.append(dir)
for line in lines:
    if line.rstrip() not in list:
	print line.rstrip()

