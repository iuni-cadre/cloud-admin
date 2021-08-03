#!/usr/bin/bash
./janus_cluster.py --cassandravm i-041dce87232dde587 10.0.1.34 --cassandravm i-0695e499d3ee4777a 10.0.1.61 --cassandravm  i-02c3c26faea6ca53b 10.0.1.46 --elasticsearchvm i-0221d597c482eecee 10.0.1.121 --janusvm  i-0e6e57cfc9f606275 10.0.1.165 stop
