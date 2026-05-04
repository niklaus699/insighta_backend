#!/usr/bin/env bash
set -e
cd /home/niklaus/hng_projects/insighta_labs/insighta_backend
pkill -f gunicorn || true
REDIS_URL='' DISABLE_RATE_LIMITS=1 nohup ./.venv/bin/gunicorn --chdir /home/niklaus/hng_projects/insighta_labs/insighta_backend -w 8 -b 127.0.0.1:8000 app:app > gunicorn_8.log 2>&1 &
echo $!
