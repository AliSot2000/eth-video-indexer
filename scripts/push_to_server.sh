#!/bin/bash

rsync -avu --stats --progress --exclude="*.bak" \
  --exclude="lin_venv/" \
  ../../eth-video-indexer \
  ubuntu-home-lab:/home/oicunt/