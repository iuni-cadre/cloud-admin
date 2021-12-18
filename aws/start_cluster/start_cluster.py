import json
import logging.config
import os
import sys
import time

from subprocess import Popen, PIPE
import boto3
from NamedAtomicLock import NamedAtomicLock

abspath = os.path.abspath(os.path.dirname(__file__))
aws_dir = os.path.dirname(abspath)
cloud_admin_dir = os.path.dirname(aws_dir)
util = cloud_admin_dir + '/util'
conf = cloud_admin_dir + '/conf'
janus = cloud_admin_dir + '/aws/ec2/janus_cluster'
script_path = janus + '/janus_cluster.py'
print(cloud_admin_dir)
sys.path.append(cloud_admin_dir)
POLL_QUEUE_SLEEP_TIME = 15

import util.config_reader

lockFile = util.config_reader.get_cluster_lock_file_name()

# Initialize the atomic lock.  Break existing locks
# upon initialization.  This script should be started
# first so it can initialize the lock before the
# shutdown script initializes.
clusterLock = NamedAtomicLock(lockFile)
clusterLock.release(forceRelease=True)

log_conf = conf + '/start-cluster-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')

logger = logging.getLogger('cadre_start_cluster')


user_logged_in_sqs_client = boto3.client('sqs',
                               aws_access_key_id=util.config_reader.get_aws_access_key(),
                               aws_secret_access_key=util.config_reader.get_aws_access_key_secret(),
                               region_name=util.config_reader.get_aws_region())

queue_url = util.config_reader.get_queue_url()
print(queue_url)
start_uspto_command = "--cassandravm i-0a42fce90cd7eb05e 10.0.1.84 --cassandravm i-02b790105f46522bd 10.0.1.165 --cassandravm  i-0e59c526105c72e53 10.0.1.250 --elasticsearchvm i-0dc077c81a34a9ebe 10.0.1.80 --vm-start-wait-time 90 --vm-status-wait-time 30 start"
start_mag_command = "--cassandravm i-05512a5f648a22c24 10.0.1.81 --cassandravm i-0a756cd826ec308cb 10.0.1.55 --cassandravm  i-0d900785b2a99cb4f 10.0.1.239 --elasticsearchvm i-0678bd7619427dc4a 10.0.1.94 --vm-start-wait-time 90 --vm-status-wait-time 30 start"
start_wos_command = "--cassandravm i-03d7229f8d4456c6e 10.0.1.87 --cassandravm i-0c2b57be36bf0d0b8 10.0.1.201 --cassandravm  i-05b04736594741634 10.0.1.119 --elasticsearchvm i-06ed04e79ba9c55c1 10.0.1.88 --vm-start-wait-time 90 --vm-status-wait-time 30 start"
python_venv_path = util.config_reader.get_python_venv_path()


def poll_queue():
    while True:
        # Receive message from SQS queue
        response = user_logged_in_sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'All'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=300,
            WaitTimeSeconds=0
        )

        

        if 'Messages' in response:
            print(response)
            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                message_body = message['Body']
                logger.info("Received message id " + message['MessageId'])
                query_json = json.loads(message_body)
                logger.info(query_json)
                dataset = query_json['dataset']
                if dataset == 'US Patent and Trademark Office patent':
                    command = start_uspto_command
                elif dataset == 'Microsoft Academic Graph':
                    command = start_mag_command
                else:
                    command = start_wos_command
                # check whether cluster already running
                # start the cluster
                try:
                    clusterLock.acquire()
                    logger.info('Launching start script with: ' + command)
                    logger.info('venv path: ' + python_venv_path)
                    logger.info('script path: ' + script_path)
                    p = Popen([python_venv_path, script_path] + command.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                    rc = p.returncode
                    logger.info('return code: ' + rc)
                    logger.info('err        : ' + err)
                    logger.info('output     : ' + output)
                except (Exception) as error:
                    logger.error(error)
                finally:
                    clusterLock.release()
                    # Delete received message from queue
                    user_logged_in_sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)

        time.sleep(POLL_QUEUE_SLEEP_TIME)

if __name__ == '__main__':
    print("Starting poll_queue")
    poll_queue()
