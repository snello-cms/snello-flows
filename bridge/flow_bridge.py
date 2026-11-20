#!/usr/bin/env python3
"""flow-bridge: where is each Quarkus Flow instance? Reads the runner's workflow events and serves them over HTTP.

The runner's API (1.3.0) lists only active instances and does not say which task they are on; its structured log does
(one JSON line per event: workflow.started / task.started / task.completed / workflow.faulted ...). The runner writes
those lines to a file too (quarkus.log.handler.file."FLOW_FILE"); this process tails the file, keeps one record per
instance (status, current task, every task position seen, error) and answers:

  GET /health
  GET /instances?namespace=&name=&version=&status=&limit=   newest first (default limit 100)
  GET /instances/{instanceId}

Payloads (workflow/task input and output) are dropped: they hold personal data and the monitor does not need them.
State is rebuilt at start from the file and its rotated backups (.1 .. .N), so nothing else is persisted.

Workflow deploy (snello admin "Workflows"; the runner 1.3.0 has no deploy API, it only watches its workflows folder):

  GET    /workflows                                   files deployed through the bridge (ui-*.yaml)
  PUT    /workflows/{namespace}/{name}/{version}      body = workflow YAML; header X-Bridge-Token
  DELETE /workflows/{namespace}/{name}/{version}      header X-Bridge-Token
  POST   /workflows/{namespace}/{name}/{version}/adopt[?dry=1]   header X-Bridge-Token; body = YAML (optional)

PUT writes WORKFLOWS_DIR/ui-{namespace}-{name}-{version}.yaml and waits until the runner has registered it (its file
watcher polls every 5 s). The runner only validates the file and logs a WARN, so its watcher log
(quarkus.log.handler.file."WATCH_FILE") is read for the error: on a WARN or a timeout the file is removed again (it
would be retried and warned about every 5 s forever) and the runner's message is returned (422 / 504).
Versions are immutable: the runner never reloads an edited file, so an already loaded namespace:name:version is 409.
DELETE only removes the file: the runner keeps a loaded workflow until it restarts.
adopt = "import into the snello editor" of a workflow the runner already runs from another file (git-managed copy):
?dry=1 answers that file's text ({yaml, source}) without writing; without dry the body (or else that file) is written
as the ui-*.yaml of the same namespace:name:version. The runner skips it ("already registered", one WARN) and loads
it from that file at its next start, so running instances keep their version. ADOPT_REMOVE_SOURCE=true also deletes
the source file(s), so only the ui-*.yaml is left (the k8s PVC; off locally, where the folder is the git checkout).
Writes are off unless WORKFLOWS_DIR and BRIDGE_WRITE_TOKEN are both set.

Env: FLOW_EVENTS_FILE (/deployments/data/flow-events.log), BRIDGE_HOST (0.0.0.0), BRIDGE_PORT (8091),
     BRIDGE_MAX_INSTANCES (5000), BRIDGE_POLL_SECONDS (0.5),
     WORKFLOWS_DIR (unset), BRIDGE_WRITE_TOKEN (unset), RUNNER_URL (http://127.0.0.1:8090),
     FLOW_WATCHER_LOG (/deployments/data/flow-watcher.log), DEPLOY_TIMEOUT_SECONDS (20), MAX_WORKFLOW_BYTES (262144),
     ADOPT_REMOVE_SOURCE (false)
Standard library only: runs on any python:3 image, the script is mounted from a ConfigMap.
"""
import hmac
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EVENTS_FILE = os.environ.get("FLOW_EVENTS_FILE", "/deployments/data/flow-events.log")
HOST = os.environ.get("BRIDGE_HOST", "0.0.0.0")
PORT = int(os.environ.get("BRIDGE_PORT", "8091"))
MAX_INSTANCES = int(os.environ.get("BRIDGE_MAX_INSTANCES", "5000"))
POLL = float(os.environ.get("BRIDGE_POLL_SECONDS", "0.5"))
PREFIX = "io.serverlessworkflow."

WORKFLOWS_DIR = os.environ.get("WORKFLOWS_DIR", "").strip()
WRITE_TOKEN = os.environ.get("BRIDGE_WRITE_TOKEN", "").strip()
RUNNER_URL = os.environ.get("RUNNER_URL", "http://127.0.0.1:8090").rstrip("/")
WATCHER_LOG = os.environ.get("FLOW_WATCHER_LOG", "/deployments/data/flow-watcher.log")
DEPLOY_TIMEOUT = float(os.environ.get("DEPLOY_TIMEOUT_SECONDS", "20"))
MAX_WORKFLOW_BYTES = int(os.environ.get("MAX_WORKFLOW_BYTES", str(256 * 1024)))
# the runner's own rule for document.namespace / document.name; versions as snello checks them
DSL_NAME = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$")
VERSION = re.compile(r"^[A-Za-z0-9.+-]{1,50}$")
UI_PREFIX = "ui-"
ADOPT_REMOVE_SOURCE = os.environ.get("ADOPT_REMOVE_SOURCE", "false").strip().lower() in ("1", "true", "yes")
deploy_lock = threading.Lock()   # one deploy at a time: each one reads "its" lines of the watcher log

logging.basicConfig(level=os.environ.get("BRIDGE_LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s [flow-bridge] %(message)s")
log = logging.getLogger("flow-bridge")

lock = threading.Lock()
instances = {}          # instanceId -> record (insertion order = first seen)
stats = {"lines": 0, "events": 0, "bad": 0, "since": time.time()}


def iso(ts):
    """Runner timestamps are epoch seconds (float) or ISO strings, depending on its config."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + f".{int((ts % 1) * 1000):03d}Z"
    return str(ts)


def record(iid):
    r = instances.get(iid)
    if r is None:
        r = {"instanceId": iid, "namespace": None, "name": None, "version": None, "status": None,
             "startedAt": None, "endedAt": None, "lastUpdate": None, "currentTask": None,
             "currentPosition": None, "tasks": {}, "error": None}
        instances[iid] = r
        if len(instances) > MAX_INSTANCES:
            oldest = next(iter(instances))
            del instances[oldest]
    return r


def apply(ev):
    """One runner event -> the instance record. Unknown event types only touch status/lastUpdate."""
    iid = ev.get("instanceId")
    kind = str(ev.get("eventType", "")).removeprefix(PREFIX).removesuffix(".v1")  # e.g. task.started
    if not iid or not kind:
        return
    r = record(iid)
    at = iso(ev.get("timestamp"))
    r["lastUpdate"] = at
    if ev.get("workflowNamespace"):
        r["namespace"], r["name"], r["version"] = ev["workflowNamespace"], ev.get("workflowName"), ev.get("workflowVersion")
    if kind == "workflow.started":
        r["startedAt"] = iso(ev.get("startTime")) or at
    if kind in ("workflow.completed", "workflow.faulted", "workflow.cancelled"):
        r["endedAt"] = iso(ev.get("endTime")) or at
        r["currentTask"] = r["currentPosition"] = None
    if kind == "workflow.completed":
        r["status"] = "COMPLETED"
    elif kind == "workflow.cancelled":
        r["status"] = "CANCELLED"
    elif kind == "workflow.faulted":
        r["status"] = "FAULTED"
    elif ev.get("status") and kind.startswith("workflow."):
        r["status"] = ev["status"]
    # only a faulted WORKFLOW is the instance's error; a faulted task may be caught and retried (try/catch + retry
    # logs task.faulted too) and then complete: that is counted on the task, not shown as the instance's error
    if kind == "workflow.faulted" and isinstance(ev.get("error"), dict):
        e = ev["error"]
        r["error"] = {"type": e.get("type"), "status": e.get("status"), "title": (e.get("title") or "")[:500],
                      "position": e.get("instance"), "at": at}
    if kind.startswith("task."):
        pos = ev.get("taskPosition")
        if not pos:
            return
        state = kind.split(".", 1)[1]  # started / completed / faulted / retried / suspended ...
        t = r["tasks"].setdefault(pos, {"name": ev.get("taskName"), "state": None, "count": 0, "faults": 0,
                                        "lastError": None, "at": None})
        t["state"], t["at"] = state, at
        if state == "started":
            t["count"] += 1
            r["currentTask"], r["currentPosition"] = ev.get("taskName"), pos
        elif state == "faulted":
            t["faults"] = t.get("faults", 0) + 1
            e = ev.get("error") if isinstance(ev.get("error"), dict) else {}
            t["lastError"] = (e.get("title") or "faulted")[:300]
            log.info("instance %s: task %s faulted (%d), %s", iid, pos, t["faults"], t["lastError"][:120])
        if r["status"] in (None, "PENDING") and state == "started":
            r["status"] = "RUNNING"


def feed(line):
    line = line.strip()
    if not line:
        return
    stats["lines"] += 1
    try:
        ev = json.loads(line)
    except ValueError:
        stats["bad"] += 1
        return
    if isinstance(ev, dict):
        with lock:
            apply(ev)
        stats["events"] += 1


def replay_backups():
    """Rotated files first, oldest (.N) to newest (.1): the state then continues with the live file."""
    n = 1
    while os.path.exists(f"{EVENTS_FILE}.{n}"):
        n += 1
    for i in range(n - 1, 0, -1):
        path = f"{EVENTS_FILE}.{i}"
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                feed(line)
        log.info("replayed %s", path)


def tail():
    """Follows EVENTS_FILE across rotation (new inode or shrunk file -> reopen from the start)."""
    replay_backups()
    fh, inode, partial = None, None, ""
    while True:
        try:
            if fh is None:
                if not os.path.exists(EVENTS_FILE):
                    time.sleep(POLL * 4)
                    continue
                fh = open(EVENTS_FILE, encoding="utf-8", errors="replace")
                inode = os.fstat(fh.fileno()).st_ino
                log.info("following %s (inode %s)", EVENTS_FILE, inode)
            chunk = fh.read()
            if chunk:
                lines = (partial + chunk).split("\n")
                partial = lines.pop()  # incomplete last line: wait for the rest
                for line in lines:
                    feed(line)
                continue
            st = os.stat(EVENTS_FILE) if os.path.exists(EVENTS_FILE) else None
            if st is None or st.st_ino != inode or st.st_size < fh.tell():
                log.info("%s rotated, reopening", EVENTS_FILE)
                if partial:
                    feed(partial)
                fh.close()
                fh, partial = None, ""
                continue
            time.sleep(POLL)
        except Exception:  # never let the tail thread die: log and retry
            log.exception("tail error, retrying")
            if fh:
                fh.close()
            fh, partial = None, ""
            time.sleep(2)


def summary(r):
    return {k: v for k, v in r.items() if k != "tasks"} | {"taskCount": len(r["tasks"])}


# ---- workflow deploy -------------------------------------------------------------------------------------------

class DeployError(Exception):
    def __init__(self, status, message, code=None):
        super().__init__(message)
        self.status, self.message, self.code = status, message, code


def writes_enabled():
    return bool(WORKFLOWS_DIR and WRITE_TOKEN)


def ui_file(ns, name, version):
    return os.path.join(WORKFLOWS_DIR, f"{UI_PREFIX}{ns}-{name}-{version}.yaml")


def runner_has(ns, name, version):
    """True / False from GET /q/flow/definitions/{ns}/{name}/{version} (200 / 404); DeployError if unreachable."""
    url = f"{RUNNER_URL}/q/flow/definitions/{ns}/{name}/{version}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=5) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise DeployError(502, f"runner answered {e.code} for {ns}:{name}:{version}")
    except OSError as e:
        raise DeployError(503, f"runner unreachable at {RUNNER_URL}: {e}")


def watcher_offset():
    try:
        return os.path.getsize(WATCHER_LOG)
    except OSError:
        return 0


def watcher_error(offset, filename):
    """First WARN/ERROR line of the runner's file watcher about `filename` written after `offset`, else None.
    The runner's lines look like: '... WARN ... File watcher — failed to parse /deployments/workflows/x.yaml — will
    retry next cycle: <cause>' or '... unexpected error processing /deployments/workflows/x.yaml: <cause>'."""
    try:
        size = os.path.getsize(WATCHER_LOG)
        with open(WATCHER_LOG, encoding="utf-8", errors="replace") as fh:
            fh.seek(offset if size >= offset else 0)  # rotated meanwhile: read the new file from the start
            for line in fh:
                if filename in line and (" WARN " in line or " ERROR " in line):
                    cause = line.split(filename, 1)[1].lstrip(" :—-")
                    cause = re.sub(r"^will retry next cycle:\s*", "", cause).strip()
                    return cause[:2000] or line.strip()[:2000]
    except OSError:
        return None
    return None


def document_ids(body):
    """namespace / name / version of the top-level `document:` block (block style, as snello saves it), without a
    YAML library: enough to refuse a body that would register under another id than the URL says."""
    ids, inside = {}, False
    for line in body.decode("utf-8", errors="replace").splitlines():
        if re.match(r"^document:\s*(#.*)?$", line):
            inside = True
            continue
        if inside:
            if line.strip() and not line[:1].isspace():
                break  # next top-level key
            m = re.match(r"^\s+(namespace|name|version):\s*[\"']?([^\"'#\s]+)", line)
            if m:
                ids.setdefault(m.group(1), m.group(2))
    return ids


def deploy(ns, name, version, body):
    """Writes the workflow file and waits for the runner: registered -> dict; refused / timeout -> DeployError."""
    ids = document_ids(body)
    if ids != {"namespace": ns, "name": name, "version": version}:
        log.warning("deploy %s:%s:%s refused: document block says %s", ns, name, version, ids)
        raise DeployError(400, f"the YAML's document namespace/name/version {ids} does not match {ns}:{name}:{version}")
    path = ui_file(ns, name, version)
    filename = os.path.basename(path)
    with deploy_lock:
        if runner_has(ns, name, version):
            log.info("deploy %s:%s:%s refused: already loaded by the runner", ns, name, version)
            raise DeployError(409, f"{ns}:{name}:{version} is already loaded by the runner: save it as a new version "
                                   "(the runner does not reload an edited workflow)")
        offset = watcher_offset()
        tmp = os.path.join(WORKFLOWS_DIR, f".{filename}.tmp")
        with open(tmp, "wb") as fh:
            fh.write(body)
        os.replace(tmp, path)  # atomic: the watcher never sees half a file
        log.info("deploy %s:%s:%s: wrote %s (%d bytes), waiting for the runner (max %.0f s)",
                 ns, name, version, path, len(body), DEPLOY_TIMEOUT)
        start = time.time()
        while time.time() - start < DEPLOY_TIMEOUT:
            time.sleep(1)
            if runner_has(ns, name, version):
                took = round(time.time() - start, 1)
                log.info("deploy %s:%s:%s: registered by the runner after %.1f s", ns, name, version, took)
                return {"namespace": ns, "name": name, "version": version, "file": filename, "seconds": took}
            err = watcher_error(offset, filename)
            if err:
                remove_quietly(path)
                log.warning("deploy %s:%s:%s: runner refused it, file removed: %s", ns, name, version, err[:300])
                raise DeployError(422, err)
        remove_quietly(path)
        log.warning("deploy %s:%s:%s: not registered within %.0f s, file removed", ns, name, version, DEPLOY_TIMEOUT)
        raise DeployError(504, f"the runner did not register {ns}:{name}:{version} within {DEPLOY_TIMEOUT:.0f} s "
                               "(does document.namespace/name/version in the YAML match? see the runner log)")


def source_files(ns, name, version):
    """Non ui-* workflow files of WORKFLOWS_DIR (top folder) whose document block is namespace:name:version."""
    want = {"namespace": ns, "name": name, "version": version}
    out = []
    if not WORKFLOWS_DIR or not os.path.isdir(WORKFLOWS_DIR):
        return out
    for f in sorted(os.listdir(WORKFLOWS_DIR)):
        path = os.path.join(WORKFLOWS_DIR, f)
        if f.startswith(UI_PREFIX) or f.startswith(".") or not f.endswith((".yaml", ".yml")) or not os.path.isfile(path):
            continue
        try:
            with open(path, "rb") as fh:
                body = fh.read(MAX_WORKFLOW_BYTES + 1)
        except OSError as e:
            log.warning("adopt: cannot read %s: %s", path, e)
            continue
        if len(body) <= MAX_WORKFLOW_BYTES and document_ids(body) == want:
            out.append((f, body))
    return out


def adopt(ns, name, version, body, dry):
    """Import of a workflow the runner already runs from another file; see the module doc."""
    if not runner_has(ns, name, version):
        log.info("adopt %s:%s:%s: not loaded by the runner", ns, name, version)
        raise DeployError(404, f"{ns}:{name}:{version} is not loaded by the runner")
    sources = source_files(ns, name, version)
    log.info("adopt %s:%s:%s: dry=%s body=%d bytes, source files %s", ns, name, version, dry, len(body or b""),
             [f for f, _ in sources] or "-")
    if not body:
        if not sources:
            raise DeployError(404, f"no workflow file for {ns}:{name}:{version} in the workflows folder "
                                   "(send the YAML in the body)", "no-source")
        body = sources[0][1]
    if dry:
        return {"namespace": ns, "name": name, "version": version, "source": sources[0][0] if sources else None,
                "yaml": body.decode("utf-8", errors="replace")}
    ids = document_ids(body)
    if ids != {"namespace": ns, "name": name, "version": version}:
        log.warning("adopt %s:%s:%s refused: document block says %s", ns, name, version, ids)
        raise DeployError(400, f"the YAML's document namespace/name/version {ids} does not match {ns}:{name}:{version}")
    path = ui_file(ns, name, version)
    with deploy_lock:
        tmp = os.path.join(WORKFLOWS_DIR, f".{os.path.basename(path)}.tmp")
        with open(tmp, "wb") as fh:
            fh.write(body)
        os.replace(tmp, path)
        removed = []
        if ADOPT_REMOVE_SOURCE:
            for f, _ in sources:
                remove_quietly(os.path.join(WORKFLOWS_DIR, f))
                removed.append(f)
    log.info("adopt %s:%s:%s: wrote %s (%d bytes), removed sources %s", ns, name, version, path, len(body),
             removed or "none (ADOPT_REMOVE_SOURCE off)")
    return {"namespace": ns, "name": name, "version": version, "file": os.path.basename(path),
            "source": sources[0][0] if sources else None, "removed": removed}


def remove_quietly(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def list_deployed():
    if not WORKFLOWS_DIR or not os.path.isdir(WORKFLOWS_DIR):
        return []
    out = []
    for f in sorted(os.listdir(WORKFLOWS_DIR)):
        if f.startswith(UI_PREFIX) and f.endswith(".yaml"):
            st = os.stat(os.path.join(WORKFLOWS_DIR, f))
            out.append({"file": f, "bytes": st.st_size, "modified": iso(st.st_mtime)})
    return out


class Handler(BaseHTTPRequestHandler):
    server_version = "flow-bridge/1.0"

    def log_message(self, fmt, *args):
        log.debug("%s %s", self.address_string(), fmt % args)

    def send_json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        q = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
        parts = [p for p in url.path.split("/") if p]
        if parts == ["health"]:
            return self.send_json(200, {"status": "UP", "file": EVENTS_FILE, "instances": len(instances),
                                        "workflowWrites": writes_enabled(), **stats})
        if parts == ["workflows"]:
            rows = list_deployed()
            log.info("GET /workflows -> %d files", len(rows))
            return self.send_json(200, {"writes": writes_enabled(), "workflows": rows})
        if parts == ["instances"]:
            try:
                limit = max(1, min(int(q.get("limit", "100")), 1000))
            except ValueError:
                return self.send_json(400, {"error": "limit must be a number"})
            with lock:
                rows = [summary(r) for r in reversed(instances.values())
                        if all(not q.get(k) or str(r.get(k) or "") == q[k] for k in ("namespace", "name", "version", "status"))]
            log.info("GET /instances %s -> %d rows", q, min(len(rows), limit))
            return self.send_json(200, {"total": len(rows), "instances": rows[:limit]})
        if len(parts) == 2 and parts[0] == "instances":
            with lock:
                r = instances.get(parts[1])
                body = json.loads(json.dumps(r)) if r else None
            log.info("GET /instances/%s -> %s", parts[1], "found" if body else "404")
            return self.send_json(200, body) if body else self.send_json(404, {"error": "unknown instance"})
        return self.send_json(404, {"error": "not found"})

    def workflow_target(self, method, suffix=None):
        """(ns, name, version) of /workflows/{ns}/{name}/{version}[/{suffix}] once writes, token and names are
        checked; None after an error answer has been sent."""
        parts = [urllib.parse.unquote(p) for p in urllib.parse.urlparse(self.path).path.split("/") if p]
        if suffix:
            if len(parts) != 5 or parts[4] != suffix:
                self.send_json(404, {"error": "not found"})
                return None
            parts = parts[:4]
        if len(parts) != 4 or parts[0] != "workflows":
            self.send_json(404, {"error": "not found"})
            return None
        if not writes_enabled():
            log.warning("%s %s: workflow writes are off (WORKFLOWS_DIR / BRIDGE_WRITE_TOKEN unset)", method, self.path)
            self.send_json(403, {"error": "workflow deploy is not enabled on this flow-bridge "
                                          "(WORKFLOWS_DIR and BRIDGE_WRITE_TOKEN)"})
            return None
        if not hmac.compare_digest(self.headers.get("X-Bridge-Token", ""), WRITE_TOKEN):
            log.warning("%s %s: bad or missing X-Bridge-Token from %s", method, self.path, self.address_string())
            self.send_json(401, {"error": "bad token"})
            return None
        ns, name, version = parts[1:4]
        if not (DSL_NAME.match(ns) and DSL_NAME.match(name) and VERSION.match(version)):
            log.warning("%s %s: invalid namespace/name/version", method, self.path)
            self.send_json(400, {"error": "namespace and name: letters, digits and '-' (not first/last), max 63; "
                                          "version: letters, digits, '.', '+', '-'"})
            return None
        return ns, name, version

    def do_PUT(self):
        target = self.workflow_target("PUT")
        if not target:
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_WORKFLOW_BYTES:
            return self.send_json(413 if length else 400, {"error": f"body: 1..{MAX_WORKFLOW_BYTES} bytes of YAML"})
        body = self.rfile.read(length)
        log.info("PUT /workflows/%s: %d bytes", ":".join(target), len(body))
        try:
            return self.send_json(201, deploy(*target, body))
        except DeployError as e:
            return self.send_json(e.status, {"error": e.message})
        except OSError as e:
            log.exception("PUT /workflows/%s: write failed", ":".join(target))
            return self.send_json(500, {"error": f"cannot write the workflow file: {e}"})

    def do_POST(self):
        target = self.workflow_target("POST", "adopt")
        if not target:
            return
        q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).items()}
        dry = q.get("dry") in ("1", "true")
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_WORKFLOW_BYTES:
            return self.send_json(413, {"error": f"body: at most {MAX_WORKFLOW_BYTES} bytes of YAML"})
        body = self.rfile.read(length) if length else b""
        if body.strip() in (b"", b"{}"):
            body = b""  # snello's empty POST body: take the source file
        log.info("POST /workflows/%s/adopt dry=%s: %d bytes", ":".join(target), dry, len(body))
        try:
            return self.send_json(200, adopt(*target, body, dry))
        except DeployError as e:
            return self.send_json(e.status, {"error": e.message, **({"code": e.code} if e.code else {})})
        except OSError as e:
            log.exception("POST /workflows/%s/adopt: write failed", ":".join(target))
            return self.send_json(500, {"error": f"cannot write the workflow file: {e}"})

    def do_DELETE(self):
        target = self.workflow_target("DELETE")
        if not target:
            return
        path = ui_file(*target)
        existed = os.path.exists(path)
        remove_quietly(path)
        log.info("DELETE /workflows/%s: %s", ":".join(target), "removed " + path if existed else "no file")
        return self.send_json(200, {"removed": existed, "file": os.path.basename(path),
                                    "note": "the runner keeps a loaded workflow until it restarts"})


def main():
    log.info("starting: events %s, http %s:%d, max %d instances", EVENTS_FILE, HOST, PORT, MAX_INSTANCES)
    if writes_enabled():
        log.info("workflow deploy ON: dir %s, runner %s, watcher log %s, timeout %.0f s",
                 WORKFLOWS_DIR, RUNNER_URL, WATCHER_LOG, DEPLOY_TIMEOUT)
    else:
        log.info("workflow deploy OFF (set WORKFLOWS_DIR and BRIDGE_WRITE_TOKEN to enable)")
    threading.Thread(target=tail, name="tail", daemon=True).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
