#!/usr/bin/python

import sys
import optparse
import boto3


def start_instances(cassandraVM, elasticsearchVM, ec2):
   print cassandraVM
   #print elasticsearchVM

   # Start Cassandra instances one at a time
   for vm in cassandraVM:
      try:
         print 'starting vm: {}'.format(vm[0])
         response = ec2.start_instances(InstanceIds=[vm[0],])
      except Exception as e:
         print 'Exception'
         sys.stderr.write(e)

   print response

def stop_instances(cassandraVM, elasticsearchVM, ec2):
   print cassandraVM
   #print elasticsearchVM
   raise Exception('Not implemented')

def process_action(action, options):
   try:
      credentials = boto3.Session().get_credentials()
      print credentials
      ec2 = boto3.client('ec2')
   except Exception:
      raise Exception('Failed to get boto EC2 client')

   if action == 'start':
      return start_instances(options.cassandraVM, options.elasticsearchVM, ec2)
   elif action == 'stop':
      return stop_instances(options.cassandraVM, options.elasticsearchVM, ec2)
   else:
      raise Exception('Invalid action \'{}\''.format(options.action))
   
def parse_args():
   prog = sys.argv[0]
   usage = 'usage: %prog [options] start|stop'
   parser = optparse.OptionParser(usage)
   parser.add_option(
      '--cassandravm', dest='cassandraVM', nargs=2, action='append',
      help='a label for a Cassandra VM and its IP address: label IP')
   parser.add_option(
      '--elasticsearchvm', dest='elasticsearchVM', nargs=2, action='append',
      help='a label for a ElasticSearch VM and its IP address: label IP')
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

   if not hasattr(options, 'cassandraVM'):
      sys.stderr.write('ERROR: Number of Cassandra VMs must be 1 or more\n')
      parser.print_help()
      return 1

   if not hasattr(options, 'elasticsearchVM'):
      sys.stderr.write('ERROR: Number of ElasticSearch VMs must be 1 or more\n')
      parser.print_help()
      return 1

   if args[0] in ('start', 'stop'):
      action = args[0]
   else:
      sys.stderr.write('ERROR: action must be start | stop\n')
      parser.print_usage()
      return 1

   try:
      code = process_action(action, options)
   except Exception as e:
      sys.stderr.write('ERROR: {}'.format(e) + '\n')

if __name__ == "__main__":
   sys.exit(main())

