import json
import logging.config
import os
import sys

import psycopg2 as psycopg2
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

abspath = os.path.abspath(os.path.dirname(__file__))
cadre = os.path.dirname(abspath)
util = cadre + '/util'
conf = cadre + '/conf'
janus = cadre + '/aws/ec2/janus_cluster'
script_path = janus + '/janus_cluster.py'
sys.path.append(cadre)

import util.config_reader
from util.db_util import cadre_meta_connection_pool

log_conf = conf + '/logging-idle-checker-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')

logger = logging.getLogger('cadre_idle_checker')

stop_uspto_command = "--cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 --janusvm  i-0e6e57cfc9f606275 10.0.1.165 stop"

def stop_uspto_cluster():
    last_logged_time_statement = "select user_id, last_update from user_token ORDER BY last_update DESC NULLS LAST LIMIT 1"
    # replace with listener_status table once it is ready
    check_active_jobs_statement = "select job_id,job_status from user_job ORDER BY modified_on DESC NULLS LAST LIMIT 1"
    meta_connection = cadre_meta_connection_pool.getconn()
    meta_db_cursor = meta_connection.cursor()
    try:
        meta_db_cursor.execute(last_logged_time_statement)
        if meta_db_cursor.rowcount > 0:
            token_last_update_info = meta_db_cursor.fetchone()
            last_active_user_id = token_last_update_info[0]
            last_active_user_time = token_last_update_info[1]
            print(last_active_user_time)
            d1 = datetime.strptime(last_active_user_time, '%Y-%m-%d %H:%M:%S.%f')
            d2 = datetime.now()
            time_difference = d2 - d1

            meta_db_cursor.execute(check_active_jobs_statement)
            if meta_db_cursor.rowcount > 0:
                active_jobs_info = meta_db_cursor.fetchone()
                status = active_jobs_info[1]
                if status != 'RUNNING' and time_difference.min > 10:
                    # can shut down the cluster
                    print("System is idle")
                    #spawn script
                    # subprocess.call(["python3", script_path,
                    #                  "--cassandravm",
                    #                  "i-041dce87232dde587 10.0.1.34",
                    #                  "--cassandravm",
                    #                  "i-0695e499d3ee4777a 10.0.1.61",
                    #                  "--cassandravm",
                    #                  "i-02c3c26faea6ca53b 10.0.1.46",
                    #                  "--elasticsearchvm",
                    #                  "i-0221d597c482eecee 10.0.1.121",
                    #                  "--janusvm",
                    #                  "i-0e6e57cfc9f606275 10.0.1.165",
                    #                  "stop"])

                    p = Popen([script_path] + stop_uspto_command.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                    rc = p.returncode
                    print(rc)
    except (Exception, psycopg2.Error) as error:
        logger.exception(error)
        logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
    finally:
        # Closing database connection.
        meta_db_cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        cadre_meta_connection_pool.putconn(meta_connection)
        logger.info("PostgreSQL connection pool is closed")


if __name__ == '__main__':
    stop_uspto_cluster()




