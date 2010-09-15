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

  try:
    # get nodes filenam from positional argument list
    nodesfilename = args[0]
  except:
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice and nodes file"
    parser.print_help()
    return -1

  # check mandatory command line args
  if not ( options.username and options.slicename and nodesfilename ):
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice and nodes file"
    parser.print_help()
    return -1

  # get password from user
  password = getpass.getpass( "planet lab password: " )

  # print summary
  print "Summary"
  print "username:  " + str(options.username)
  print "slicename: " + str(options.slicename)
  print "nodesfile: " + str(nodesfilename)
  print ""

  try:
    # read all nodes into list
    node_list = [line.strip() for line in open(nodesfilename)]
  except:
    print "Error in reading nodes file " + str(nodesfilename)
    return -2

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
    print ""

    print "Adding " + str(len(node_list)) + " nodes to slice..."
    # add nodes to slice
    api_server.AddSliceToNodes(auth, options.slicename, node_list)
    print "...Done"
  except:
    print "Error in adding nodes to slice " + str(options.slicename)
    return -3

  # return success
  return 0

if __name__ == "__main__":
  sys.exit(main())
