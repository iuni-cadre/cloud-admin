import json
import logging.config
import os
import sys
import time

import psycopg2 as psycopg2
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
import time
from NamedAtomicLock import NamedAtomicLock

# Find the directory where this utility is installed
# and locate the configuration files
abspath = os.path.abspath(os.path.dirname(__file__))
aws_dir = os.path.dirname(abspath)
cloud_admin_dir = os.path.dirname(aws_dir)
util = cloud_admin_dir + '/util'
conf = cloud_admin_dir + '/conf'
janus = cloud_admin_dir + '/aws/ec2/janus_cluster'
script_path = janus + '/janus_cluster.py'
sys.path.append(cloud_admin_dir)
INIT_SLEEP_TIME = 10

import util.config_reader
from util.db_util import cadre_meta_connection_pool

lockFile = util.config_reader.get_cluster_lock_file_name()

# Initialize the atomic lock.
clusterLock = NamedAtomicLock(lockFile)

log_conf = conf + '/logging-idle-checker-conf.json'
with open(log_conf, 'r') as logging_configuration_file:
    config_dict = json.load(logging_configuration_file)

logging.config.dictConfig(config_dict)

# Log that the logger was configured
logger = logging.getLogger(__name__)
logger.info('Completed configuring logger()!')

logger = logging.getLogger('cadre_idle_checker')

stop_uspto_command = "--cassandravm i-0a42fce90cd7eb05e 10.0.1.84 --cassandravm i-02b790105f46522bd 10.0.1.165 --cassandravm  i-0e59c526105c72e53 10.0.1.250 --elasticsearchvm i-0dc077c81a34a9ebe 10.0.1.80 stop"
stop_mag_command = "--cassandravm i-05512a5f648a22c24 10.0.1.81 --cassandravm i-0a756cd826ec308cb 10.0.1.55 --cassandravm  i-0d900785b2a99cb4f 10.0.1.239 --elasticsearchvm i-0678bd7619427dc4a 10.0.1.94 stop"
stop_wos_command = "--cassandravm i-01918d2d5395c098e 10.0.1.64 --cassandravm i-0466a1b02975594f0 10.0.1.238 --cassandravm  i-044a9190c9375e9f5 10.0.1.78 --elasticsearchvm i-03182b9a3213ee347 10.0.1.216 stop"
command = stop_uspto_command
python_venv_path = util.config_reader.get_python_venv_path()


def stop_uspto_cluster():
    last_logged_time_statement = "SELECT user_id, last_update FROM user_token ORDER BY last_update DESC NULLS LAST LIMIT 1"
    # replace with listener_status table once it is ready
    check_listener_running_statement = "SELECT last_cluster,status,last_report_time FROM listener_status WHERE status='RUNNING' AND last_cluster='%s'"
    cluster_list=['WOS', 'MAG', 'USPTO']

    meta_connection = cadre_meta_connection_pool.getconn()
    meta_db_cursor = meta_connection.cursor()
    try:
        logger.info('acquiring cluster lock')
        clusterLock.acquire()
        logger.info('acquired cluster lock')
        meta_db_cursor.execute(last_logged_time_statement)
        if meta_db_cursor.rowcount > 0:
            token_last_update_info = meta_db_cursor.fetchone()
            last_active_user_id = token_last_update_info[0]
            last_active_user_time = token_last_update_info[1]
            #logger.info(last_active_user_time)
            #logger.info(type(last_active_user_time))
            d2 = datetime.now()
            active_user_elapsed_time = time.mktime(d2.timetuple()) - time.mktime(last_active_user_time.timetuple())
            active_user_elapsed_time /= 60.0
            logger.info('Elapsed time since last interface user: ' + str(active_user_elapsed_time) + 'min')

            for cluster in cluster_list:
                logger.info('Checking cluster status for ' + cluster)
                if cluster == "USPTO":
                   shutdown_cmd = stop_uspto_command
                elif cluster == "WOS":
                   shutdown_cmd = stop_wos_command
                elif cluster == "MAG":
                   shutdown_cmd = stop_mag_command
                else:
                   raise Exception("Unknown dataset '%s' detected" % cluster)

                meta_db_cursor.execute(check_listener_running_statement % cluster)
                logger.info('Number of running ' + cluster + ' listeners: ' + str(meta_db_cursor.rowcount))
                if meta_db_cursor.rowcount == 0:
                    logger.info('There are no running listeners for ' + cluster)
                    # there is no running jobs
                    check_listener_idle_statement = "SELECT last_cluster,status,last_report_time FROM listener_status WHERE last_cluster='%s' AND status <> 'STOPPED' ORDER BY last_report_time DESC NULLS LAST LIMIT 1"
                    meta_db_cursor.execute(check_listener_idle_statement % cluster)
                    if meta_db_cursor.rowcount > 0:
                        logger.info("There is at least one running '" + cluster + "' listener.  Checking for last update time of listener.")
                        idle_listener_info = meta_db_cursor.fetchone()
                        listener_last_update_time = idle_listener_info[2]
                        listener_last_update_time_difference = time.mktime(d2.timetuple()) - time.mktime(listener_last_update_time.timetuple())
                        #logger.info(listener_last_update_time_difference)
                        listener_last_update_time_difference /= 60.0
                        logger.info("Time since a '" + cluster + "' listener was last updated: "
                           + str(listener_last_update_time_difference) + 'min')
                        if listener_last_update_time_difference > 10:
                        # We no longer consider if a user is in the interface before shutting a cluster down.
                        # If a cluster is idle, shut it down regardless of whether or not a user is in the interface.
                        #if listener_last_update_time_difference > 10 and active_user_elapsed_time > 10:
                            # can shut down the cluster
                            logger.info("Cluster '" + cluster + "'is idle")
                            #command_list = [stop_uspto_command, stop_mag_command, stop_wos_command]
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

                            logger.info('Shutting down cluster ' + cluster + ' with -- ' + shutdown_cmd)
                            p = Popen([python_venv_path, script_path] + shutdown_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                            output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                            rc = p.returncode
                            logger.info('return code: ' + str(rc))
                            logger.info('err        : ' + str(err))
                            logger.info('output     : ' + str(output))
                    else:
                        logger.info('Shutting down cluster ' + cluster + ' with no last_cluster status using -- ' + shutdown_cmd)
                        p = Popen([python_venv_path, script_path] + shutdown_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                        output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                        rc = p.returncode
                        logger.info('return code: ' + str(rc))
                        logger.info('err        : ' + str(err))
                        logger.info('output     : ' + str(output))
    except (Exception, psycopg2.Error) as error:
        print(error)
        logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
    finally:
        clusterLock.release()
        # Closing database connection.
        meta_db_cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        cadre_meta_connection_pool.putconn(meta_connection)
        logger.info("PostgreSQL connection pool is closed")


if __name__ == '__main__':
    time.sleep(INIT_SLEEP_TIME)
    stop_uspto_cluster()




