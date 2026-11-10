#!/usr/bin/env bash
# Drives the echo example through a real Bowire host, the way an operator
# and the workbench's UI do, over one transport:
#
#   scripts/e2e.sh stdio <echo-binary>   the host spawns the sidecar
#   scripts/e2e.sh http  <echo-binary>   the sidecar runs on its own and the
#                                        manifest points the host at it
#
# Both install the example the way an operator does, as a sidecar zip, and
# run the same checks: one contract, two wires. The binary for http needs
# the http feature (cargo build --release --features http --example echo).
#
# Needs `bowire` (the Bowire CLI), curl, jq and Python (for the zip; set
# PYTHON if it is not `python3`) on PATH. Every wait is on a signal: a
# line the sidecar or the host prints once it accepts connections, and SSE
# requests returning when the host ends the stream.

set -euo pipefail

transport=$1
bin=$2
id=echo-rs
target="$id@http://localhost:1"
work=$(mktemp -d)
plugins="$work/plugins"
here=$(cd "$(dirname "$0")/.." && pwd)
sidecar=

# Waits for the first line of $1 matching $2 and prints it.
await_line() {
  timeout 60 grep -m1 "$2" <(tail -n +1 -f "$1")
}

stop_all() {
  [ -n "${host:-}" ] && kill "$host" 2>/dev/null || true
  [ -n "$sidecar" ] && kill "$sidecar" 2>/dev/null || true
  echo "--- host log"; cat "$work/host.log" 2>/dev/null || true
  echo "--- sidecar log"; cat "$work/sidecar.log" 2>/dev/null || true
}
trap stop_all EXIT

echo "== package the example as a $transport sidecar"
mkdir -p "$work/stage"
case "$transport" in
  stdio)
    cp "$bin" "$work/stage/"
    cp "$here/examples/echo/sidecar.json" "$work/stage/"
    ;;
  http)
    # The host does not own an http sidecar: start it here, on a port the
    # OS picks, and read the port off the line it prints once it listens.
    "$bin" --http 127.0.0.1:0 > "$work/sidecar.log" 2>&1 &
    sidecar=$!
    endpoint=$(await_line "$work/sidecar.log" "listening on" | grep -o 'http://[^ ]*')
    echo "sidecar at $endpoint"
    jq --arg url "$endpoint" 'del(.executable, .envPrefix, .shutdownTimeoutMs)
        | .packageId += ".Http" | .transport = "http" | .url = $url' \
      "$here/examples/echo/sidecar.json" > "$work/stage/sidecar.json"
    ;;
  *)
    echo "transport must be stdio or http, not '$transport'" >&2
    exit 2
    ;;
esac
cat "$work/stage/sidecar.json"
(cd "$work/stage" && "${PYTHON:-python3}" -m zipfile -c "$work/echo-rs.zip" ./*)
bowire --plugin-dir "$plugins" plugin install --file "$work/echo-rs.zip"
bowire --plugin-dir "$plugins" plugin list --verbose

echo "== discover through the CLI"
out=$(bowire --plugin-dir "$plugins" discover --url "$target")
echo "$out"
echo "$out" | grep -q "DemoService"

echo "== start the workbench"
bowire --plugin-dir "$plugins" --no-browser --port 0 > "$work/host.log" 2>&1 &
host=$!
banner=$(await_line "$work/host.log" "Bowire is running at")
url=$(echo "$banner" | grep -o 'http://[^ ]*' | sed 's:/*$::')
echo "host at $url"
mkdir -p "$work/ws"
# The host reads this path itself; under Git Bash on Windows it needs the
# Windows form of the MSYS one.
ws=$(cygpath -m "$work/ws" 2>/dev/null || echo "$work/ws")
q="workspaceId=e2e&storageRoot=$ws&serverUrl=$target"

# Settings end to end: the declaration reaches the Settings dialog's
# endpoint with the options a select needs, and a value set in a workspace
# reaches the plugin on the next call, then falls back to the default once
# cleared. The CLI has no workspace, so this goes through the workbench.
echo "== settings"
curl -sf "$url/api/protocols?$q" | jq -e '.[] | select(.id == "echo-rs") | .settings[] | select(.key == "transform")
           | .type == "select" and ([.options[].value] == ["none","upper","lower"])'

echo_hi() {
  curl -sf -X POST "$url/api/invoke?$q" -H 'content-type: application/json' \
    -d '{"protocol":"echo-rs","service":"DemoService","method":"Echo","messages":["{\"message\":\"hi\"}"]}' \
    | jq -r .response
}
echo_hi | grep -q '"message":"hi"'
curl -sf -X PUT "$url/api/plugins/settings/echo-rs/transform?$q" \
  -H 'content-type: application/json' -d '{"value":"upper"}' > /dev/null
echo_hi | grep -q '"MESSAGE":"HI"'
curl -sf -X PUT "$url/api/plugins/settings/echo-rs/transform?$q" \
  -H 'content-type: application/json' -d '{"value":null}' > /dev/null
echo_hi | grep -q '"message":"hi"'

# A server stream: the workbench's stream endpoint, which runs the host's
# invokeStream. Over http its frames reach the host as $/stream/data on the
# sidecar's SSE stream, and $/stream/end is what ends this request.
echo "== server stream"
messages=$(jq -rn '[{"message":"hi"} | tojson] | tojson | @uri')
curl -sfN "$url/api/invoke/stream?$q&protocol=$id&service=DemoService&method=Count&messages=$messages" \
  > "$work/stream.txt"
cat "$work/stream.txt"
grep '^data: ' "$work/stream.txt" | sed 's/^data: //' | jq -s -e \
  '[.[] | select(.data != null) | .data | fromjson] | [.[].n] == [1,2,3] and all(.echoed.message == "hi")'
grep -q '^event: done' "$work/stream.txt"

# A duplex method. The CLI has no duplex mode (`bowire call --stream`
# drives invokeStream, not openChannel), so this goes through the
# workbench's channel endpoints exactly as the browser UI does: open, read
# the SSE stream, send twice, close. The stream ending with `event: done`
# is the host having received `$/channel/closed`.
echo "== duplex channel"
open=$(curl -sf -X POST "$url/api/channel/open?serverUrl=$target" \
  -H 'content-type: application/json' \
  -d '{"service":"DemoService","method":"Chat","protocol":"echo-rs"}')
echo "$open"
channel=$(echo "$open" | jq -er .channelId)

curl -sfN "$url/api/channel/$channel/responses" > "$work/responses.txt" &
sse=$!
for word in one two; do
  curl -sf -X POST "$url/api/channel/$channel/send" -H 'content-type: application/json' \
    -d "{\"message\":\"{\\\"message\\\":\\\"$word\\\"}\"}" | jq -e '.sent == true'
done
curl -sf -X POST "$url/api/channel/$channel/close" | jq -e '.closed == true'
wait $sse
cat "$work/responses.txt"
grep -q 'echoed\\":{\\"message\\":\\"one' "$work/responses.txt"
grep -q 'echoed\\":{\\"message\\":\\"two' "$work/responses.txt"
grep -q '^event: done' "$work/responses.txt"

# The hand-entry form the example declares (Kuestenlogik/Bowire#1003). A
# host before 2.11 has no ManualEntry service (404): a note, not a failure,
# until the released tool is 2.11.
echo "== hand-entry form"
status=$(curl -s -o "$work/forms.json" -w '%{http_code}' "$url/api/v1/manual-entry-forms?pageSize=200")
if [ "$status" = "404" ]; then
  echo "skipped, this host predates Bowire 2.11"
else
  jq -e '.forms[] | select(.protocol == "echo-rs")
         | [.callKinds[] | "\(.id):\(.methodType)"] == ["echo:Unary","count:ServerStreaming","chat:Duplex"]' "$work/forms.json"
fi

# An http sidecar is not the host's to stop, so it has to stop when told to,
# while the host still holds its SSE stream open. `wait` returns the
# sidecar's exit code: 0 is a clean shutdown.
if [ -n "$sidecar" ]; then
  echo "== stop the http sidecar, host still connected"
  kill -TERM "$sidecar"
  wait "$sidecar"
  sidecar=
fi

kill "$host"
wait "$host" 2>/dev/null || true
host=

trap - EXIT
echo "== all $transport checks passed"
