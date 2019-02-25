#!/bin/bash

start-yarn.sh && start-dfs.sh && mr-jobhistory-daemon.sh --config /usr/local/hadoop/etc/hadoop start historyserver #  && (yarn timelineserver 2>&1 > ~/timeline.log &)
