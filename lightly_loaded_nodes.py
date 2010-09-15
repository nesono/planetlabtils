#!/usr/bin/python

import urllib
import sys

def main():
  # the select string
  select_str = 'resptime>0&&1 minload<5&&liveslices<=5'
  # open url and read all text
  all_text = urllib.urlopen('http://comon.cs.princeton.edu/status/tabulator.cgi?table=table_nodeviewshort&format=nameonly&select=\'' + select_str + '\'').read()

  # open output file and write all text
  open('nodelist_light_load.txt', 'w').write( all_text )

  return 0

if __name__ == "__main__":
  sys.exit(main())
