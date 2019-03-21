#!/usr/bin/env bash

for i in $(seq 0 $1)
do
    systemd-cat -t btbtt06Thread$i python -u btbtt06Threads.py &
    echo "$!"
    sleep 1
done

echo

for i in $(seq 0 $2)
do
    systemd-cat -t btbtt06Atts$i python -u btbtt06Atts.py &
    echo "$!"
    sleep 1
done