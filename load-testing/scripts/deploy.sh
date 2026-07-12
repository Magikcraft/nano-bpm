#!/bin/bash
# deploy.sh - swap in nano-gw-new + wipe-restart all 3 nodes on OOTB defaults
# (leader-durable, RF3, 12 partitions, adaptive rails; the verification launcher).
# Usage: deploy.sh <MAXBKLOG|default|off> [CAP] [LIVENESS]
SSHK="-i /home/joshua.wulf/.ssh/google_compute_engine -o StrictHostKeyChecking=no -o ConnectTimeout=10"
NODES="10.128.0.19 10.128.0.20 10.128.0.18"
MAXBKLOG="${1:-default}"; CAP="${2:-100000}"; LIVENESS="${3:-600000}"
LB64="IyEvYmluL2Jhc2gKIyBWZXJpZmljYXRpb24gbGF1bmNoZXI6IE5FVyBiaW5hcnkgKG5hbm8tZ3ctbmV3IC0+IG5hbm8tZ3cpIHVuZGVyIGxlYWRlci1kdXJhYmxlCiMgd2l0aCB0aGUgTkVXIERFRkFVTFRTLiBEb2VzIE5PVCBzZXQgTkFOT0JQTU5fUkVQTElDQVRFX0FDVElWQVRJT04gKHNvIHRoZSBuZXcKIyBtb2RlLWRlcGVuZGVudCBkZWZhdWx0IGFwcGxpZXM6IGxlYWRlci1sb2NhbCB1bmRlciBsZWFkZXItZHVyYWJsZSkuIEFkbWlzc2lvbgojIGJhY2tsb2cgaXMgcGFyYW1ldGVyaXplZDogImRlZmF1bHQiIG9taXRzIHRoZSBlbnYgKGFkYXB0aXZlIGJhY2tzdG9wKSwgIm9mZiIKIyBkaXNhYmxlcywgYSBudW1iZXIgc2V0cyBhbiBleHBsaWNpdCBwZXItbm9kZSBjYXAuCnNldCAtZQpNQVhCS0xPRz0iJHsxOi1kZWZhdWx0fSIKQ0FQPSIkezI6LTEwMDAwMH0iCkxJVkVORVNTPSIkezM6LTYwMDAwMH0iCk5JRD0iJHtIT1NUTkFNRSMjKi19IgpOT0RFUz0iaHR0cDovLzEwLjEyOC4wLjE5OjgwODAsaHR0cDovLzEwLjEyOC4wLjIwOjgwODAsaHR0cDovLzEwLjEyOC4wLjE4OjgwODAiCmNwIC1mICIkSE9NRS9uYW5vLWd3LW5ldyIgIiRIT01FL25hbm8tZ3ciCkJLTE9HX0FSRz0oKQpjYXNlICIkTUFYQktMT0ciIGluCiAgZGVmYXVsdCkgOiA7OyAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICMgb21pdCAtPiBhZGFwdGl2ZSBkZWZhdWx0CiAgKikgICAgICAgQktMT0dfQVJHPSgtLXNldGVudj1OQU5PQlBNTl9BRE1JU1NJT05fTUFYX0JBQ0tMT0c9IiRNQVhCS0xPRyIpIDs7CmVzYWMKc3VkbyBzeXN0ZW1kLXJ1biAtLXVuaXQ9bmFubyAtLWNvbGxlY3QgXAogIC0tdWlkPSQoaWQgLXUpIC0tZ2lkPSQoaWQgLWcpIFwKICAtLXNldGVudj1IT01FPSRIT01FIC0tc2V0ZW52PVBPUlQ9ODA4MCBcCiAgLS1zZXRlbnY9TkFOT0JQTU5fTk9ERVM9IiROT0RFUyIgLS1zZXRlbnY9TkFOT0JQTU5fTk9ERV9JRD0iJE5JRCIgXAogIC0tc2V0ZW52PU5BTk9CUE1OX1JGPTMgLS1zZXRlbnY9TkFOT0JQTU5fUEFSVElUSU9OUz0xMiBcCiAgLS1zZXRlbnY9TkFOT0JQTU5fSk9VUk5BTD1zZWdtZW50ZWQgLS1zZXRlbnY9TkFOT0JQTU5fTEVBTl9TTkFQU0hPVD0xIFwKICAtLXNldGVudj1OQU5PQlBNTl9EQVRBX0RJUj0kSE9NRS9uYW5vLWRhdGEgXAogIC0tc2V0ZW52PU5BTk9CUE1OX1ZBUl9TUElMTD1hZGFwdGl2ZSAtLXNldGVudj1OQU5PQlBNTl9WQVJfU1BJTExfTUI9NzAwIFwKICAtLXNldGVudj1OQU5PQlBNTl9DT0xEX1NQSUxMPWFkYXB0aXZlIC0tc2V0ZW52PU5BTk9CUE1OX0NPTERfU1BJTExfTUI9NzAwIFwKICAtLXNldGVudj1OQU5PQlBNTl9ISVNUT1JZX1JFVEVOVElPTj1hZGFwdGl2ZSAtLXNldGVudj1OQU5PQlBNTl9ISVNUT1JZX1JFVEVOVElPTl9NQj02MDAwIFwKICAtLXNldGVudj1OQU5PQlBNTl9FWFBPUlRFUl9RVUVVRT1hZGFwdGl2ZSAtLXNldGVudj1OQU5PQlBNTl9NRU1fV0FURVJNQVJLPWFkYXB0aXZlIFwKICAtLXNldGVudj1OQU5PQlBNTl9SQUZUPTEgLS1zZXRlbnY9TkFOT0JQTU5fUkVQTElDQVRJT049bGVhZGVyLWR1cmFibGUgXAogIC0tc2V0ZW52PU5BTk9CUE1OX0RVUkFCSUxJVFk9c3luYyAtLXNldGVudj1OQU5PQlBNTl9KT1VSTkFMX0xJTkdFUl9VUz0wIFwKICAtLXNldGVudj1OQU5PQlBNTl9TTEFfTU9ERT1sYXRlbmN5IFwKICAiJHtCS0xPR19BUkdbQF19IiBcCiAgLS1zZXRlbnY9TkFOT0JQTU5fQURNSVNTSU9OX01BWF9DUkVBVEVfUVVFVUU9IiRDQVAiIFwKICAtLXNldGVudj1OQU5PQlBNTl9TVFJFQU1fTElWRU5FU1NfTVM9IiRMSVZFTkVTUyIgXAogICRIT01FL25hbm8tZ3cKc2xlZXAgMwpzeXN0ZW1jdGwgaXMtYWN0aXZlIG5hbm8uc2VydmljZSAmJiBlY2hvICJMQVVOQ0hFRC1WRVJJRlkgbm9kZS0kTklEIHJlcGxpY2F0ZV9hY3RpdmF0aW9uPURFRkFVTFQgbWF4QmFja2xvZz0kTUFYQktMT0cgbGl2ZW5lc3M9JExJVkVORVNTIHNoYSAkKHNoYTI1NnN1bSB+L25hbm8tZ3d8Y3V0IC1jMS0xNikiCg=="
for ip in $NODES; do
  echo "--- deploy+wipe $ip ---"
  ssh $SSHK $ip "echo '$LB64' | base64 -d > ~/node-launch-verify.sh && chmod +x ~/node-launch-verify.sh; for p in \$(pgrep -x nano-gw); do kill \$p; done; for i in \$(seq 1 40); do pgrep -x nano-gw >/dev/null || break; sleep 1; done; if pgrep -x nano-gw >/dev/null; then for p in \$(pgrep -x nano-gw); do kill -9 \$p; done; fi; sleep 1; rm -rf ~/nano-data; echo wiped-\$(pgrep -x nano-gw|wc -l)"
done
# --- disk preflight: refuse to launch a soak without headroom (prevents ENOSPC crash) ---
GUARD="$(dirname "$0")/disk-guard.sh"; [ -x "$GUARD" ] || GUARD="$HOME/disk-guard.sh"
if [ -x "$GUARD" ]; then
  "$GUARD" preflight "${DISK_MIN_FREE_GB:-40}" || { echo "ABORT: disk preflight failed (see above)"; exit 1; }
fi
echo "=== staggered start (leader-durable, DEFAULT activation, maxBacklog=$MAXBKLOG) ==="
for ip in $NODES; do
  ssh $SSHK $ip "nohup ~/node-launch-verify.sh $MAXBKLOG $CAP $LIVENESS >~/nano-launch.log 2>&1 & echo started-$ip"
  sleep 8
done
sleep 4
for ip in $NODES; do ssh $SSHK $ip "cat ~/nano-launch.log 2>/dev/null | tail -1"; done
echo "=== DEPLOY DONE ==="
