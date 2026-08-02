#!/bin/sh

CONFIG_DIR="${CONFIG_DIR:-/usr/src/app/config}"

run_once() {
  if [ -d "${CONFIG_DIR}" ] && ls "${CONFIG_DIR}"/*.y*ml >/dev/null 2>&1; then
    for config in "${CONFIG_DIR}"/*.y*ml; do
      echo "=== Running elsewherr.py with config: ${config} ==="
      python /usr/src/app/elsewherr.py --config "${config}"
    done
  else
    python /usr/src/app/elsewherr.py
  fi
}

if [ "$1" = "once" ]; then
  run_once
  exit 0
fi

if [ "${SCHEDULE}" = "None" ]; then
  run_once
else
  echo "${SCHEDULE} CONFIG_DIR=${CONFIG_DIR} /usr/src/app/run.sh once" | crontab - && crond -f -L /dev/stdout
fi
