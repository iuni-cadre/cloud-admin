import json
import logging.config
import os
import sys

import psycopg2 as psycopg2
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
import time

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
from util.db_util import cadre_meta_connection_pool

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
stop_wos_command = "--cassandravm i-03d7229f8d4456c6e 10.0.1.87 --cassandravm i-0c2b57be36bf0d0b8 10.0.1.201 --cassandravm  i-05b04736594741634 10.0.1.119 --elasticsearchvm i-06ed04e79ba9c55c1 10.0.1.88 stop"
command = stop_uspto_command
python_venv_path = util.config_reader.get_python_venv_path()


def stop_uspto_cluster():
    last_logged_time_statement = "SELECT user_id, last_update FROM user_token ORDER BY last_update DESC NULLS LAST LIMIT 1"
    # replace with listener_status table once it is ready
    check_listener_running_statement = "SELECT last_cluster,status,last_report_time FROM listener_status WHERE status='RUNNING'"

    meta_connection = cadre_meta_connection_pool.getconn()
    meta_db_cursor = meta_connection.cursor()
    try:
        meta_db_cursor.execute(last_logged_time_statement)
        if meta_db_cursor.rowcount > 0:
            token_last_update_info = meta_db_cursor.fetchone()
            last_active_user_id = token_last_update_info[0]
            last_active_user_time = token_last_update_info[1]
            print(last_active_user_time)
            print("88888888888888")
            print(type(last_active_user_time))
            d2 = datetime.now()
            print(d2)
            time_difference = time.mktime(d2.timetuple()) - time.mktime(last_active_user_time.timetuple())
            print(d2)
            print(time_difference)

            meta_db_cursor.execute(check_listener_running_statement)
            print(meta_db_cursor.rowcount)
            if meta_db_cursor.rowcount == 0:
                print("no running listeners")
                # there is no running jobs
                check_listener_idle_statement = "SELECT last_cluster,status,last_report_time FROM listener_status  ORDER BY last_report_time DESC NULLS LAST LIMIT 1"
                meta_db_cursor.execute(check_listener_idle_statement)
                if meta_db_cursor.rowcount > 0:
                    print("check for last updated time of listener")
                    idle_listener_info = meta_db_cursor.fetchone()
                    listener_last_updated_time = idle_listener_info[2]
                    dataset = idle_listener_info[0]
                    listner_last_update_time_difference = time.mktime(d2.timetuple()) - time.mktime(listener_last_updated_time.timetuple())
                    print(listner_last_update_time_difference / 60.0)
                    if listner_last_update_time_difference > 10 and time_difference > 10:
                        # can shut down the cluster
                        print("System is idle")
                        if dataset == "USPTO":
                            command_list = [stop_uspto_command]
                        elif dataset == "WOS":
                            command_list = [stop_wos_command]
                        elif dataset == "MAG":
                            command_list = [stop_mag_command]
                        else:
                            command_list = [stop_uspto_command, stop_mag_command, stop_wos_command]

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

                        for command in command_list:
                            p = Popen([python_venv_path, script_path] + command.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
                            output, err = p.communicate(b"input data that is passed to subprocess' stdin")
                            rc = p.returncode
                            print(rc)
                            print(output)
                            print(err)
    except (Exception, psycopg2.Error) as error:
        print(error)
        logger.error('Error while connecting to PostgreSQL. Error is ' + str(error))
    finally:
        # Closing database connection.
        meta_db_cursor.close()
        # Use this method to release the connection object and send back ti connection pool
        cadre_meta_connection_pool.putconn(meta_connection)
        logger.info("PostgreSQL connection pool is closed")


if __name__ == '__main__':
    stop_uspto_cluster()




