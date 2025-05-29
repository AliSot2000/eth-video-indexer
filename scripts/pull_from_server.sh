#!/bin/bash

rsync -avu --stats --progress --exclude="*.bak" \
  --exclude="lin_venv/" \
  --exclude="*__pycache__/*" \
  --exclude=".git/*"
  --exclude=".idea/*"
  ubuntu-home-lab:/home/oicunt/eth-video-indexer/ \
  ../../eth-video-indexer/