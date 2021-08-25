#!/usr/bin/env bash
pushd /home/ubuntu/cloud-admin
source venv/bin/activate
exec python aws/start_cluster/start_cluster.py