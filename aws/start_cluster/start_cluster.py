import json
import logging.config
import os
import sys

from subprocess import Popen, PIPE
import boto3

abspath = os.path.abspath(os.path.dirname(__file__))
aws_dir = os.path.dirname(abspath)
cloud_admin_dir = os.path.dirname(aws_dir)
util = cloud_admin_dir + '/util'
conf = cloud_admin_dir + '/conf'
janus = cloud_admin_dir + '/aws/ec2/janus_cluster'
script_path = janus + '/janus_cluster.py'
print(cloud_admin_dir)
sys.path.append(cloud_admin_dir)

import util.config_reader

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
start_uspto_command = "--cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 start"
start_mag_command = "--cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 start"
start_wos_command = "--cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 start"
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
                    command =  start_mag_command
                else:
                    command = start_wos_command
                # check whether cluster already running
                # start the cluster
                try:
                    p = Popen([python_venv_path, script_path] + command.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                    rc = p.returncode
                    print(rc)
                    print(err)
                    print(output)
                except (Exception) as error:
                    logger.error(error)
                finally:
                    # Delete received message from queue
                    user_logged_in_sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info('Received and deleted message: %s' % message)


if __name__ == '__main__':
    poll_queue()