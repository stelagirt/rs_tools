#!/bin/bash

set -o xtrace

SAVE_DIR="/mnt/seviri/"

START_DATE="2021-09-01" # repeat download for cloudmask!!!
END_DATE="2021-09-30"
TIME_STEP="00:15:00"

# python rs_tools/_src/data/msg/downloader_msg_modis_overpass.py --save-dir $SAVE_DIR --start-date $START_DATE --end-date $END_DATE
python /home/sgirtsou/Projects/rs_tools/rs_tools/_src/data/msg/downloader_msg.py --save-dir $SAVE_DIR --start-date $START_DATE --end-date $END_DATE --time-step $TIME_STEP