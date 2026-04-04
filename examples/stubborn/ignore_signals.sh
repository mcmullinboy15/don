#!/bin/sh
# A stubborn process that traps SIGTERM and SIGINT and refuses to die.
trap '' TERM INT
echo "I will never die (PID $$)"
i=0
while true; do
    echo "still alive... ($i)"
    i=$((i + 1))
    sleep 2
done
