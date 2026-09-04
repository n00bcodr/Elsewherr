#!/bin/sh

CONFIG_DIR="${CONFIG_DIR:-/usr/src/app/config}"

run_once() {
  if [ -d "${CONFIG_DIR}" ] && ls "${CONFIG_DIR}"/*.y*ml >/dev/null 2>&1; then
    for config in "${CONFIG_DIR}"/*.y*ml; do
      echo "=== Running elsewherr.py with config: ${config} ==="
      /usr/local/bin/python /usr/src/app/elsewherr.py --config "${config}"
    done
  else
    /usr/local/bin/python /usr/src/app/elsewherr.py
  fi
}

if [ "$1" = "once" ]; then
  run_once
  exit 0
fi

if [ "${SCHEDULE}" = "None" ]; then
  run_once
else
  CRON_LOG=/var/log/cron.log
  touch "${CRON_LOG}"
  echo "${SCHEDULE} CONFIG_DIR=${CONFIG_DIR} /usr/src/app/run.sh once >> ${CRON_LOG} 2>&1" | crontab -
  cron
  tail -f "${CRON_LOG}"
fi
