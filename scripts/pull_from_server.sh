#!/bin/bash

rsync -avu --stats --progress --exclude="*.bak" \
  --exclude="lin_venv/" \
  --exclude="*__pycache__/*" \
  192.168.1.20:/home/oicunt/eth-video-indexer/ \
  ../../eth-video-indexer/