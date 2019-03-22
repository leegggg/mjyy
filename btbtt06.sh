#!/usr/bin/env bash

for i in $(seq 0 $2)
do
    systemd-cat -t btbtt06Atts$i python -u btbtt06DownloadImages.py &
    echo "$!"
    sleep 1
done