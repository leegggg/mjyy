#!/usr/bin/env bash

for i in $(seq 0 8)
do
    systemd-cat -t btbtt06Thread$i python -u btbtt06Threads.py &
done

for i in $(seq 0 4)
do
    systemd-cat -t btbtt06Atts$i python -u btbtt06Atts.py &
done