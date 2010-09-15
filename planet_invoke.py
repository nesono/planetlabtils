#!/usr/bin/env python

import xmlrpclib
import sys
from optparse import OptionParser
import getpass
import os
import subprocess
import threading, Queue
import signal
import time

# the queue for the remote nodes to synchronize
node_queue = Queue.Queue()
bad_nodes  = Queue.Queue()
# helper variables
slicename  = ''
local_dir  = ''
remote_dir = ''
# global stop flag - join() doesn't work with threads and signal
g_stop_flag = False

def handler( signum, frame ):
  print 'Signal handler called with signal', signum
  print 'flushing queue, please wait'
  print 'this can take up to 10 Seconds'
  global g_stop_flag
  global node_queue

  g_stop_flag = True
  try:
    while not node_queue.empty():
      # flush queue
      #print "trashing task"
      trash = node_queue.get()
      #print "trashing task done"
      node_queue.task_done()
      #print "trashing finished"
  except:
    print "flushing aborted"

  print "queue flushed"
  sys.exit(1)

def work_method():
  while True:
    global g_stop_flag
    if g_stop_flag == True:
      break
    global bad_nodes
    global node_queue

    # get next item from queue (peer hostname)
    cmd = node_queue.get()
    #print "Calling: " + cmd
    cmdpopen = subprocess.Popen( cmd, shell=True, close_fds=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE )

    # get stdout and stderr from popen object
    cmd_stdout, cmd_stderr = cmdpopen.communicate()

    try_no = 1
    # poll for subprocess to be finishded
    while True:
      cmdpopen.poll()
      # check if process finished
      if cmdpopen.returncode != None:
        break
      #print "waiting for process"
      # wait for some time
      time.sleep(1)
      # wait for 10 seconds max
      if try_no >= 10:
        try:
          bad_nodes.put( cmd )
          cmdpopen.terminate()
          print "child terminated"
        except:
          print str(cmd) + ":"
          print "could not terminate child, abandon it\n"
        break
      try_no += 1

    print str(cmd), ":\nretcode:\n", str(cmdpopen.returncode), "\nstdout:\n", cmd_stdout, "stderr:\n", cmd_stderr, "\n"

    node_queue.task_done()

def main():
  global bad_nodes
  global node_queue

  # create option parser instance
  parser = OptionParser()
  parser.add_option( "-u", "--user", dest="username", help="planet lab username" )
  parser.add_option( "-s", "--slice", dest="slicename", help="planet lab slice name" )
  parser.add_option( "-j", "--threads", dest="threads", help="number of threads used for directory synchronization (default: 20)" )
  (options, args) = parser.parse_args()

  # get positional arguments
  try:
    command = args[0]
  except:
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice and command"
    parser.print_help()
    return -1

  if not options.threads:
    nofthreads = 20
  else:
    nofthreads = int(options.threads)

  if nofthreads <= 0:
    nofthreads = 1

  if not ( options.username and options.slicename and command ):
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice command"
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
  print "command:    " + str(command)
  print "threads:    " + str(nofthreads)
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

  # create worker threads
  for iter in range(nofthreads):
    new_thread = threading.Thread( target=work_method )
    new_thread.setDaemon( True )
    new_thread.start()

  # go through all nodes and rsync them
  for node_item in node_list:
    # get hostname from node struct list
    node = node_item['hostname']

    # call rsync
    compound_command = 'ssh -t ' + slicename + '@' + node + ' "' + command + '"'

    # add hostname to  queue
    node_queue.put( compound_command )

  # this is actually a join...
  while not node_queue.empty():
    global g_stop_flag
    if g_stop_flag == True:
      print "stop flag was set - exiting"
      break
    #print "waiting..."
    time.sleep( 1 )
  # wait for all workers to finish
  node_queue.join()

  # to remember, if we retried bad nodes
  retried = 0

  while bad_nodes.qsize() > 0:
    print "Number of bad nodes: " + str(bad_nodes.qsize())
    print ""

    ans = raw_input('Shall I retry with bad nodes? [y/N] ')

    if ans == 'y' or ans == 'Y':
      retried = 1
      # retry the bad nodes
      while not bad_nodes.empty():
        node_queue.put( bad_nodes.get() )
    else:
      break

  # this is actually a join...
  while not node_queue.empty():
    if g_stop_flag == True:
      print "stop flag was set - exiting"
      break
    #print "waiting..."
    time.sleep( 1 )
  # wait for all workers to finish
  node_queue.join()

  if bad_nodes.qsize() > 0:
    if retried == 1:
      print "Number of bad nodes: " + str(bad_nodes.qsize())
      print ""

    ans = raw_input('Shall I remove bad nodes from slice? [Y/n] ')
    if ans == 'N' or ans == 'n':
      print "NOT removing bad nodes from slice\n"
    else:
      print "removing bad nodes from slice\n"
      while not bad_nodes.empty():
        if api_server.DeleteSliceFromNodes( auth, options.slicename, [bad_nodes.get()] ) == 1:
          print '*',
          sys.stdout.flush()

  # return success
  return 0


if __name__ == "__main__":
  # add the signal handler
  signal.signal(signal.SIGTERM, handler)
  signal.signal(signal.SIGINT, handler)

  sys.exit(main())
