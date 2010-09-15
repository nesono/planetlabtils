#!/usr/bin/env python

import xmlrpclib
import sys
from optparse import OptionParser
import getpass
import os
import subprocess


def main():
  # create option parser instance
  parser = OptionParser()
  parser.add_option( "-i", "--start", action="store_const", const=1, dest="start", help="start service" )
  parser.add_option( "-o", "--stop", action="store_const", const=1, dest="stop", help="stop service" )

  parser.add_option( "-M", "--minute", dest="minute", help="minute field for cron", default='*' )
  parser.add_option( "-H", "--hour", dest="hour", help="hour field for cron", default='*' )
  parser.add_option( "-d", "--day", dest="day", help="day field for cron", default='*' )
  parser.add_option( "-w", "--weekday", dest="weekday", help="day of week field for cron", default='*' )

  parser.add_option( "-u", "--user", dest="username", help="planet lab username" )
  parser.add_option( "-s", "--slice", dest="slicename", help="planet lab slice name" )

  # parse arguments
  (options, args) = parser.parse_args()

  # check for mandatory non-positional arguments
  if options.start and options.stop:
    print "Error: specify start and stop exclusively!"
    parser.print_help()
    return -1
  if not (options.start or options.stop):
    print "Mandatory argument missing!"
    print "List of mandatory arguments:"
    print "(start or stop), user, slice"
    parser.print_help()
    return -1

  service_path=''
  if options.start == 1:
    # get service path
    try:
      service_path = args[0]
    except:
      print "service path empty, please specify service!"
      return -1
  else:
    # check service path
    try:
      service_path = args[0]
    except:
      service_path = ""
    else:
      print "ignoring service path in stop mode"

  # cron fields if not set
  if options.start   ==  1  and \
     options.minute  == '*' and \
     options.hour    == '*' and \
     options.day     == '*' and \
     options.weekday == '*':
    print "all cron fields set to '*' means run every minute"
    ans = raw_input("Proceed anyway? [y/N] ")
    if ans == '' or ans.lower() == 'n':
      print "cancelled by user request"
      return -2

  if not ( options.username and options.slicename ):
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice"
    parser.print_help()
    return -2

  # get password from user
  password = getpass.getpass( "planet lab password: " )

  # set global slicename
  slicename = options.slicename

  # print summary
  print "Summary"
  print "username:   " + str(options.username)
  print "slicename:  " + str(slicename)
  print ""

  try:
    # connect to the planet lab API
    api_server = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/')

    # fill in the authentication instance
    auth = {}
    auth['Username'] = options.username
    auth['AuthString'] = password
    auth['AuthMethod'] = "password"

    # get all nodes of slice
    node_id_list = api_server.GetSlices(auth, options.slicename, ['node_ids'])[0]['node_ids']
    print "Slice " + options.slicename + " has " + str(len(node_id_list)) + " nodes"
    print ""

    # get names of nodes
    node_list = api_server.GetNodes(auth, node_id_list, ['hostname'])

  except:
    print "Error in getting nodes of slice " + str(options.slicename)
    print "Is the authentication information correct?"
    return -3

  # built cron line
  cronline  = options.minute + ' '
  cronline += options.hour + ' '
  cronline += options.day + ' '
  # any month
  cronline += '* '
  cronline += options.weekday

  # append command
  cronline += ' ' + service_path

  invoke_cmd = ''
  # built planet_invoke command line
  if options.start == 1:
    invoke_cmd += '"echo \'' + cronline + '\' | crontab - ; sudo /sbin/service crond start"'
  else:
    invoke_cmd += '"sudo /sbin/service crond stop"'

  # go through all nodes and rsync them
  for node_item in node_list:
    # get hostname from node struct list
    node = node_item['hostname']

    # call rsync
    compound_command = 'ssh -t ' + slicename + '@' + node + ' ' + invoke_cmd

    # invoke query
    print "calling node ", node, "with command: ",  invoke_cmd
    cmdpopen = subprocess.Popen( compound_command, shell=True )
    cmdpopen.wait()

  return 0

if __name__ == "__main__":
  # add the signal handler
  #signal.signal(signal.SIGTERM, handler)
  #signal.signal(signal.SIGINT, handler)

  sys.exit(main())
