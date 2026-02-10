#!/bin/bash

set -x  # Enable command printing

# build the dockerfile
DOCKER_CLI_HINTS=false DOCKER_BUILDKIT=1 docker build --tag {{ docker_name }} .

# -t means allocate a pseudo-TTY, necessary to keep it running in the background
docker run -t --detach -p 7474:7474 -p 7687:7687 -p 8773:8773 --name {{ docker_name }} {{ docker_name }}:latest
