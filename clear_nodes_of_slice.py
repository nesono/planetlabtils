#!/usr/bin/python

import xmlrpclib
import sys
from optparse import OptionParser
import getpass

def main():
  # create option parser instance
  parser = OptionParser()
  parser.add_option( "-u", "--user", dest="username", help="planet lab username" )
  parser.add_option( "-s", "--slice", dest="slicename", help="planet lab slice name" )
  (options, args) = parser.parse_args()

  # check mandatory command line args
  if not (options.username and options.slicename):
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user and slice"
    parser.print_help()
    return -1

  # get password from user
  password = getpass.getpass( "planet lab password: " )

  # print summary
  print "Summary"
  print "username:  " + str(options.username)
  print "slicename: " + str(options.slicename)
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
    nodes_todel = api_server.GetSlices(auth, options.slicename, ['node_ids'])[0]['node_ids']
    print "Slice " + options.slicename + " has " + str(len(nodes_todel)) + " nodes"

    if len(nodes_todel) != 0:
      # remove slice from all nodes
      api_server.DeleteSliceFromNodes(auth, options.slicename, nodes_todel )
      print "Slice " + options.slicename + " HAD " + str(len(nodes_todel)) + " nodes"
    else:
      print "Nothing to remove... Exiting"


  except:
    print "Error in deleting all nodes from slice " + str(options.slicename)
    return -3

  # return success
  return 0

if __name__ == "__main__":
  sys.exit(main())
