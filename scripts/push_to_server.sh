#!/bin/bash

rsync -avu --stats --progress --exclude="*.bak" \
  --exclude="lin_venv/" \
  ../../eth-video-indexer \
  192.168.1.20:/home/oicunt/