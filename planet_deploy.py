#!/usr/bin/env python

import xmlrpclib
import sys
from optparse import OptionParser
import getpass
import os
import subprocess
import threading, Queue
import random
import signal
import time

# the queue for the remote nodes to synchronize
node_queue = Queue.Queue()
# helper variables
bad_nodes  = Queue.Queue()
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
  g_stop_flag = True
  try:
    while not node_queue.empty():
      # flush queue
      trash = node_queue.get()
      node_queue.task_done()
  except:
    print "flushing aborted"

  print "queue flushed"
  sys.exit(1)


def rsync_upload():
  while True:
    global g_stop_flag
    if g_stop_flag == True:
      break
    # get next item from queue (peer hostname)
    node = node_queue.get()
    # global variables
    global slicename
    global local_dir
    global remote_dir
    global bad_nodes

    # call rsync
    remote_compound = slicename + '@' + node.strip() + ':' + '~/' + str(remote_dir).strip()
    command = '/usr/bin/rsync -avz --timeout=10 --delete ' + local_dir + ' ' + remote_compound
    #print "Calling: " + command
    cmdpopen = subprocess.Popen( command, shell=True, close_fds=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE )

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
          bad_nodes.put( node )
          cmdpopen.terminate()
          print "child terminated"
        except:
          print str(node) + ":"
          print "could not terminate child, abandon it\n"
        continue
      try_no += 1

    # get stdout and stderr from command
    cmd_stdout, cmd_stderr = cmdpopen.communicate()
    print str(node) + ":\nstdout:\n" + cmd_stdout + \
          "stderr:\n" + cmd_stderr + "\n"

    # check result of process
    if cmdpopen.returncode != 0:
      # or add to fail list
      print "rsync failed with return value: " + str(cmdpopen.returncode)
      bad_nodes.put( node )

    ## for testing purposes only (add 10% of nodes to bad nodes)
    #if random.random() <= 0.1:
    #  bad_nodes.put( node )
    node_queue.task_done()

def main():
  # global variables
  global slicename
  global local_dir
  global remote_dir
  global bad_nodes

  # create option parser instance
  parser = OptionParser()
  parser.add_option( "-u", "--user", dest="username", help="planet lab username" )
  parser.add_option( "-s", "--slice", dest="slicename", help="planet lab slice name" )
  parser.add_option( "-j", "--threads", dest="threads", help="number of threads used for directory synchronization (default: 5)" )
  (options, args) = parser.parse_args()

  # get positional arguments
  try:
    local_dir  = args[0]
  except:
    print "local dir not specified!"

  try:
    remote_dir = args[1]
  except:
    remote_dir = ''
    print "remote dir set to ${HOME}"

  if not options.threads:
    nofthreads = 5
  else:
    nofthreads = int(options.threads)

  if nofthreads <= 0:
    nofthreads = 1

  if not ( options.username and options.slicename and local_dir ):
    print "Error in parsing command line: not all mandatory parameters given!"
    print "Mandatry parameters: user, slice, local_dir and remote_dir"
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
  print "local dir:  " + str(local_dir)
  print "remote dir: " + str(remote_dir)
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
    new_thread = threading.Thread( target=rsync_upload )
    new_thread.setDaemon( True )
    new_thread.start()

  # go through all nodes and rsync them
  for node_item in node_list:
    # get hostname from node struct list
    node = node_item['hostname']

    # call rsync
    remote_compound = slicename + '@' + node.strip() + ':' + '~/' + str(remote_dir).strip()
    command = '/usr/bin/rsync -avz --delete ' + local_dir + ' ' + remote_compound

    # add hostname to  queue
    node_queue.put( node )

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
