#!/usr/bin/python3

import sys
import time
import optparse
import boto3

class ActionException(Exception):
   def __init__(self, value):
      self.parameter = value

   def __str__(self):
      return repr(self.parameter)

def get_start_state(response):
   states = []
   si = response['StartingInstances']

   for instance in si:
      cs = instance['CurrentState']
      states.append(cs['Name'])

   return states

def get_stop_state(response):
   states = []
   si = response['StoppingInstances']

   for instance in si:
      cs = instance['CurrentState']
      states.append(cs['Name'])

   return states

def get_http_status(response):
   m = response['ResponseMetadata']
   code = m['HTTPStatusCode']
   return code

def build_identifier_list(idList, optionList):
   for vm in optionList:
      idList.append(vm[0])

   return idList

def pending_or_running(state):
   ret = True

   for s in state:
      if not s in ('pending', 'running'):
         ret = False
         break

   return ret

def pending_or_stopping(state):
   ret = True

   for s in state:
      if not s in ('pending', 'stopping', 'stopped'):
         ret = False
         break

   return ret


def running(state):
   ret = True

   for s in state:
      if s != 'running':
         ret = False
         break

   return ret

# Start the VMs in vmList one at a time with a wait time of
# vmStartWaitTime in between each startup.
def start_sequential(vmList, tag, options, ec2):
   if tag == 'Cassandra':
      vmStartWaitTime = options.cassandraVMStartWaitTime
   elif tag == 'ElasticSearch':
      vmStartWaitTime = options.elasticSearchVMStartWaitTime
   elif tag == 'Janus':
      vmStartWaitTime = options.janusSearchVMStartWaitTime
   else:
      raise Exception('Invalid vmList tag value {}.  Must be one of [\'Cassandra\', \'ElasticSearch\', \'Janus\']'.format(tag))
     
      
   #print('start_sequential vmList')
   #print(vmList)
   for vm in vmList:

      # Sometimes the call to start_instances will throw an
      # exception for no discernable reason.  Retry until it
      # doesn't throw an exception or until the retry limit
      # is reached.  The retries are also useful for handling
      # edge cases where the VM might be started right after
      # an attempt to stop it; retrying will give the AWS API
      # time to catch up with abrupt starts and stops.
      numStartAttempts = 1
      startAttempt = True
      while startAttempt and (numStartAttempts <= options.vmNumActionAttempts):
         try:
            #print('starting vm: {}'.format(vm[0]))
            response = ec2.start_instances(InstanceIds=[vm[0],])

            httpCode = get_http_status(response)

            if (httpCode != 200):
               #print('response HTTP code: {}'.format(httpCode))
               continue

            # Sleep for the estimated time needed for the VM to
            # reach the 'running' state.
            time.sleep(vmStartWaitTime)
            startState = get_start_state(response)

            if startState[0] in ('pending', 'running'):
               #print('Status of VM {} is: {}'.format(vm[0], startState[0]))
               # No need to try again
               startAttempt = False

               if startState[0] == 'pending':
                  numStatusRetries = 1
                  statusRetry = True

                  while statusRetry and (numStatusRetries <= options.vmNumStatusRetries):
                     try:
                        #print('Updating status of VM {}'.format(vm[0]))
                        time.sleep(options.vmStatusWaitTime)
                        # Get the status of the started/starting VM
                        response = ec2.start_instances(InstanceIds=[vm[0],])
                        startState = get_start_state(response)

                        if startState[0] == 'running':
                           #print('Status of VM {} is now running'.format(vm[0]))
                           statusRetry = False
                        else:
                           numStatusRetries += 1
                     except:
                        numStatusRetries += 1

                  if startState[0] != 'running':
                     raise ActionException('VM: \'{}\' status: {}'.format(vm[0], startState[0]))
            else:
               numStartAttempts += 1
               
         except ActionException as e:
            raise e
         except Exception as e:
            sys.stderr.write('WARN: start_instances exception : {}'.format(e))
            numStartAttempts += 1

      if numStartAttempts > options.vmNumActionAttempts:
         raise Exception('EC2 API failed to initiate start for one or more {} VMs'.format(tag))
 

# Start the VMs in vmList concurrently.
def start_concurrent(vmList, tag, options, ec2): 

   if tag == 'Cassandra':
      vmStartWaitTime = options.cassandraVMStartWaitTime
   elif tag == 'ElasticSearch':
      vmStartWaitTime = options.elasticSearchVMStartWaitTime
   elif tag == 'Janus':
      vmStartWaitTime = options.janusSearchVMStartWaitTime
   else:
      raise Exception('Invalid vmList tag value {}.  Must be one of [\'Cassandra\', \'ElasticSearch\', \'Janus\']'.format(tag))

   vmList = build_identifier_list([], vmList)
   #print('start_concurrent vmList')
   #print(vmList)
   numStartAttempts = 1
   startAttempt = True
   while startAttempt and (numStartAttempts <= options.vmNumActionAttempts):
      try:
         #print('numStartAttempts: {}'.format(numStartAttempts))
         response = ec2.start_instances(InstanceIds=vmList)
         httpCode = get_http_status(response)

         if (httpCode != 200):
            continue

         # Sleep for the estimated time needed for the VMs to
         # reach the 'running' state.
         time.sleep(vmStartWaitTime)
         startState = get_start_state(response)

         if pending_or_running(startState):
            # No need to try again
            startAttempt = False

            if 'pending' in startState:
               numStatusRetries = 1
               statusRetry = True

               while statusRetry and (numStatusRetries <= options.vmNumStatusRetries):
                  try:
                     #print('numStatusRetries: {}'.format(numStatusRetries))
                     time.sleep(options.vmStatusWaitTime)
                     # Get the status of the started/starting VM
                     response = ec2.start_instances(InstanceIds=vmList)
                     startState = get_start_state(response)

                     if not 'pending' in startState:
                        statusRetry = False
                     else:
                        numStatusRetries += 1
                  except:
                     numStatusRetries += 1

               if not running(startState):
                  raise ActionException('One or more {} VMs failed to start'.format(tag))
            else:
               numStartAttempts += 1
         else:
            numStartAttempts += 1

      except ActionException as e:
         raise e
      except Exception as e:
         sys.stderr.write('WARN: start_instances exception : {}'.format(e))
         numStartAttempts += 1
      
   if numStartAttempts > options.vmNumActionAttempts:
      raise Exception('API failed to initiate start for one or more {} VMs'.format(tag))


def start_instances(options, ec2):
   start_sequential(options.cassandraVM, 'Cassandra', options, ec2)

   #Start the ElasticSearch VMs
   #print('Calling start_concurrent on elastic search')
   start_concurrent(options.elasticsearchVM, 'ElasticSearch', options, ec2)

   if options.janusVM != None:
      #print('calling start_sequential on Janus')
      time.sleep(options.vmStatusWaitTime)
      start_sequential(options.janusVM, 'Janus', options, ec2)

# Stop all instances and wait for the VM states to enter
# stopping, stopped, or pending.
def stop_instances(options, ec2):
   # Start the ElasticSearch VMs and Janus VM
   vmList = []

   if options.janusVM != None:
      vmList = build_identifier_list(vmList, options.janusVM)

   vmList = build_identifier_list(vmList, options.elasticsearchVM)
   vmList = build_identifier_list(vmList, options.cassandraVM)

   numStopAttempts = 1
   stopAttempt = True
   while stopAttempt and (numStopAttempts <= options.vmNumActionAttempts):
      try:
         response = ec2.stop_instances(InstanceIds=vmList)
         time.sleep(options.vmStatusWaitTime)
         httpCode = get_http_status(response)

         if (httpCode != 200):
            continue

         # Sleep for the estimated time needed for the VMs to
         # reach the 'stopped' state.
         stopState = get_stop_state(response)

         if pending_or_stopping(stopState):
            # No need to try again
            stopAttempt = False
         else:
            numStopAttempts += 1

      except Exception as e:
         numStopAttempts += 1
      
   if numStopAttempts > options.vmNumActionAttempts:
      raise Exception('API failed to initiate stop for ElasticSearch and Janus VMs')


def process_action(action, options):
   try:
      credentials = boto3.Session().get_credentials()
      ec2 = boto3.client('ec2')
   except Exception:
      raise Exception('Failed to get boto EC2 client')

   if action == 'start':
      try:
         start_instances(options, ec2)
      except Exception as e:
         try:
            stop_instances(options, ec2)
         except:
            pass
         sys.stderr.write('ERROR: Failed to start a VM -- ' + str(e) + '\n') 
         raise
   elif action == 'stop':
      try:
         stop_instances(options, ec2)
      except Exception as e:
         sys.stderr.write('ERROR: Failed to stop a VM -- ' + str(e) + '\n') 
         raise
   else:
      raise Exception('Invalid action \'{}\''.format(options.action))
   
def parse_args():
   prog = sys.argv[0]
   usage = 'usage: %prog [options] start|stop'
   parser = optparse.OptionParser(usage)
   parser.add_option(
      '--cassandravm', dest='cassandraVM', nargs=2, action='append',
      help='an identifier for a Cassandra VM and its IP address: identifier IP. Option can be used multiple times to append VMs to a list.')
   parser.add_option(
      '--elasticsearchvm', dest='elasticsearchVM', nargs=2, action='append',
      help='an identifier for a ElasticSearch VM and its IP address: identifier IP.  Option can be used multiple times to append VMs to a list.')
   parser.add_option(
      '--janusvm', dest='janusVM', nargs=2, action='append',
      help='an identifier for a Janus server front end and its IP address: identifier IP.')
   parser.add_option(
      '--vm-num-action-attempts', dest='vmNumActionAttempts', type='int',
      default=3, action='store',
      help='number of status updates if a VM failed to start/stop after <vm-start-wait-time>')
   parser.add_option(
      '--cassandra-vm-start-wait-time', dest='cassandraVMStartWaitTime', type='int',
      default=30, action='store',
      help='time to wait (in seconds) for a Cassandra VM to start')
   parser.add_option(
      '--elasticsearch-vm-start-wait-time', dest='elasticSearchVMStartWaitTime', type='int',
      default=120, action='store',
      help='time to wait (in seconds) for a ElasticSearch VM to start')
   parser.add_option(
      '--janus-vm-start-wait-time', dest='janusVMStartWaitTime', type='int',
      default=30, action='store',
      help='time to wait (in seconds) for a Janus VM to start')
   parser.add_option(
      '--vm-status-wait-time', dest='vmStatusWaitTime', type='int',
      default=30, action='store',
      help='wait time (in seconds) between VM status updates if a VM and its services failed to fully start/stop after <vm-start-wait-time>')
   parser.add_option(
      '--vm-num-status-retries', dest='vmNumStatusRetries', type='int',
      default=3, action='store',
      help='number of status updates if a VM failed to start/stop after <vm-start-wait-time> or if an action failed for an unknown reason')
   options, args = parser.parse_args()

   return parser, options, args

def main():
   try:
      parser, options, args = parse_args()
   except Exception as e:
      sys.stderr.write('ERROR: {}'.format(e) + '\n')
      return 1

   # Options and argument error checking
   if len(args) != 1:
      sys.stderr.write('ERROR: Number of positional arguments must be 1\n')
      parser.print_usage()
      return 1

   if options.cassandraVM == None:
      sys.stderr.write('ERROR: Number of Cassandra VMs must be 1 or more\n')
      parser.print_help()
      return 1

   if options.elasticsearchVM == None:
      sys.stderr.write('ERROR: Number of ElasticSearch VMs must be 1 or more\n')
      parser.print_help()
      return 1

   if args[0] in ('start', 'stop'):
      action = args[0]
   else:
      sys.stderr.write('ERROR: action must be start | stop\n')
      parser.print_usage()
      return 1

   code = 2

   try:
      print('janus_cluster processing action ' + '\'' + action + '\'')
      process_action(action, options)
      code = 0
   except Exception as e:
      sys.stderr.write('ERROR: {}'.format(e) + '\n')

   return code

if __name__ == "__main__":
   sys.exit(main())

