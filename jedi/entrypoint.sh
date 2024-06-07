#!/usr/bin/env bash
sleep 10

python3 sync_project/sync.py &
python3 generate_dags/yoda.py &



tail -f /dev/null
