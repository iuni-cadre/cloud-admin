import json
import logging.config
import os
import sys

import psycopg2 as psycopg2
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
import boto3

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
janus = cadre + '/aws/ec2/janus_cluster'
script_path = janus + '/janus_cluster.py'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool

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
start_uspto_command = "--cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 --janusvm  i-0e6e57cfc9f606275 10.0.1.165 start"

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
            for message in response['Messages']:
                receipt_handle = message['ReceiptHandle']
                message_body = message['Body']
                logger.info("Received message id " + message['MessageId'])
                query_json = json.loads(message_body)
                logger.info(query_json)
                # check whether cluster already running
                # start the cluster
                try:
                    p = Popen([script_path] + start_uspto_command.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                    rc = p.returncode
                    print(rc)
                except (Exception) as error:
                    logger.error(error)
                finally:
                    # Delete received message from queue
                    user_logged_in_sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)
