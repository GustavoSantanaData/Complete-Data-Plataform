#!/usr/bin/env bash
sleep 10

python3 cron_script.py &

tail -f /dev/null
