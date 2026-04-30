#!/bin/bash
# Heartbeat check — pure bash/sqlite replacement for Claude scheduled_task 47d28261.
#
# Queries agent_heartbeats in the local claudeclaw.db for the 9 spoke agents
# Rudy coordinates, prints a table of last-seen timestamps + status, then
# appends a "STALE: <agent> <duration>" line for any agent whose
# last_heartbeat is older than 30 min (matches the 5-min writer cadence).
#
# Pure report. No side effects. No restarts. No notifications. Designed to be
# run by system cron (see crontab) at minutes 7,37 during 7am-9pm PT.
#
# Exit 0 always — scheduler should not retry on "stale agents found". That's
# a signal, not a failure.

set -euo pipefail

DB="/Users/aditya_office_ai_assistant/claudeclaw/store/claudeclaw.db"
STALE_THRESHOLD=1800  # 30 min in seconds (writer ticks every 5 min)
SPOKE_FILTER="agent_id IN ('main','ops','builder','content','research','s2l','qa','rainmaker','trader')"

if [ ! -f "$DB" ]; then
  echo "ERROR: db not found at $DB"
  exit 0
fi

echo "=== Heartbeat check $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

# Table view of the 9 spoke agents, newest heartbeat first.
sqlite3 -header -column "$DB" <<SQL
SELECT
  agent_id,
  agent_name,
  status,
  datetime(last_heartbeat, 'unixepoch', 'localtime') AS last_seen
FROM agent_heartbeats
WHERE $SPOKE_FILTER
ORDER BY last_heartbeat DESC;
SQL

# Stale agents: last_heartbeat older than threshold. "STALE: <agent> <duration>".
NOW=$(date +%s)
sqlite3 "$DB" <<SQL | while IFS='|' read -r agent_id seconds_silent; do
SELECT agent_id, ($NOW - last_heartbeat)
FROM agent_heartbeats
WHERE $SPOKE_FILTER
  AND ($NOW - last_heartbeat) > $STALE_THRESHOLD
ORDER BY last_heartbeat ASC;
SQL
  [ -z "$agent_id" ] && continue
  # Format duration as "Xh Ym" for readability.
  hours=$(( seconds_silent / 3600 ))
  minutes=$(( (seconds_silent % 3600) / 60 ))
  echo "STALE: $agent_id ${hours}h ${minutes}m"
done

exit 0
