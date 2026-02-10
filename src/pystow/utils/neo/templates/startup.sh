#!/bin/bash

set -x  # Enable command printing

neo4j start

# Get the port
until [ "$(curl -s -w '%{http_code}' -o /dev/null "http://localhost:7474")" -eq 200 ]
do
  sleep 5
done

neo4j status
{{ python }} -m uvicorn --host 0.0.0.0 --port 8773 --factory semra.wsgi:get_app
