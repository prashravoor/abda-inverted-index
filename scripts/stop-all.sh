#!/bin/bash


mr-jobhistory-daemon.sh --config /usr/local/hadoop/etc/hadoop stop historyserver; stop-dfs.sh && stop-yarn.sh; killall timelineserver
