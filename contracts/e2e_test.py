#!/usr/bin/env python3
"""End-to-end check of the rent contracts flow against the local stack, one PASS/FAIL line per requirement.

Runs three contracts in parallel: A = ACCEPT, B = REFUSE, C = ACCEPT -> UNDO (after the workflow saw it) -> REFUSE.
Mails are read from the snello log (run snello with SNELLO_MAIL_MOCK=true: the mock mailer logs every mail).

Env: SNELLO_LOG (snello quarkus:dev output, required), SNELLO_URL, RUNNER_URL, REVIEWER (fiorenzino@gmail.com),
SNELLO_ADMIN_USER / SNELLO_ADMIN_PASSWORD (admin / admin, local dev realm), MINIO_USER / MINIO_PASSWORD (minio / minio123).
Usage: SNELLO_LOG=/path/snello.log python3 -I contracts/e2e_test.py      (about 3 minutes)
"""
import concurrent.futures
import json
import os
import pathlib
import re
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid as uuidlib

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
SNELLO = os.environ.get("SNELLO_URL", "http://localhost:8080").rstrip("/")
RUNNER = os.environ.get("RUNNER_URL", "http://127.0.0.1:8090").rstrip("/")
LOG = pathlib.Path(os.environ["SNELLO_LOG"])
REVIEWER = os.environ.get("REVIEWER", "fiorenzino@gmail.com")
TOKEN_URL = "http://localhost:8180/realms/snello/protocol/openid-connect/token"
RUN = uuidlib.uuid4().hex[:6]  # tags this run's applicants, so old log lines never match

results = []
lock = threading.Lock()


def log(msg):
    with lock:
        print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def check(req, ok, detail=""):
    with lock:
        results.append((req, bool(ok), detail))
        print(f"{'PASS' if ok else 'FAIL'}  {req}" + (f"  — {detail}" if detail else ""), flush=True)
    return ok


def token():
    body = urllib.parse.urlencode({"grant_type": "password", "client_id": "snello-api",
                                   "username": os.environ.get("SNELLO_ADMIN_USER", "admin"),
                                   "password": os.environ.get("SNELLO_ADMIN_PASSWORD", "admin")}).encode()
    with urllib.request.urlopen(urllib.request.Request(TOKEN_URL, body)) as r:
        return json.load(r)["access_token"]


TOKEN = token()


def http(method, url, body=None, auth=True, raw=False, headers=None):
    # raw = a page or a file: send */* like a browser (Accept: application/json gets 406 there)
    h = {"Accept": "*/*" if raw else "application/json", **(headers or {})}
    if auth:
        h["Authorization"] = "Bearer " + TOKEN
    data = None
    if body is not None and not isinstance(body, bytes):
        data, h["Content-Type"] = json.dumps(body).encode(), "application/json"
    elif body is not None:
        data = body
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data, h, method=method)) as r:
            b = r.read()
            return r.status, (b if raw else (json.loads(b) if b.strip() else None)), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace"), e.headers


def sql(query):
    out = subprocess.run(["docker", "exec", "snello-dev-postgresql-1", "psql", "-U", "snello", "-d", "snello",
                          "-tAF", "|", "-c", query], capture_output=True, text=True, check=True).stdout.strip()
    return out


def wait_for(fn, timeout, every=2):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(every)
    return fn()


def mails():
    """Mock mailer log entries: subject, to, cc, html."""
    text = LOG.read_text(errors="replace")
    out = []
    for m in re.finditer(r"Sending email (.*?) from \S+ to \[(.*?)\] \(cc: \[(.*?)\], bcc: \[.*?\]\), text body.*?"
                         r"html body: \n(.*?)\n\d{4}-\d\d-\d\d ", text, re.S):
        out.append({"subject": m.group(1), "to": m.group(2), "cc": m.group(3), "html": m.group(4)})
    return out


def find_mail(to, subject_part, body_part=""):
    return next((m for m in mails() if to in m["to"] and subject_part in m["subject"] and body_part in m["html"]), None)


# ---------------------------------------------------------------- infrastructure
def check_infra():
    insp = json.loads(subprocess.run(["docker", "inspect", "flow-runner"], capture_output=True, text=True).stdout)[0]
    mounts = {m["Destination"]: (m["Source"], m["RW"]) for m in insp["Mounts"]}
    check("runner: quarkus-flow-runner running locally", insp["State"]["Running"], insp["Config"]["Image"])
    check("runner: workflows/ mounted", "/deployments/workflows" in mounts, str(mounts.get("/deployments/workflows")))
    check("runner: data/ mounted (writable, H2 MVStore)", mounts.get("/deployments/data", (0, 0))[1]
          and (ROOT / "data" / "flowRunnerData.mv").exists(), str(mounts.get("/deployments/data")))
    before = set((ROOT / "backups").glob("*.mv")) if (ROOT / "backups").exists() else set()
    r = subprocess.run([str(ROOT / "backup.sh")], capture_output=True, text=True)
    new = set((ROOT / "backups").glob("*.mv")) - before
    check("runner: H2 backup (backup.sh) works and runner stays up",
          r.returncode == 0 and new and http("GET", RUNNER + "/q/health/ready", auth=False)[0] == 200,
          ", ".join(p.name for p in new) or r.stderr.strip())
    s, d, _ = http("GET", RUNNER + "/q/flow/definitions/contracts/contract-evaluation/1.0.0", auth=False)
    check("workflow contracts:contract-evaluation loaded (contract_evaluation is not a legal DSL name)", s == 200)

    def fields(table):
        return {f["name"]: f for f in http("GET", f"{SNELLO}/api/datalist/metadata/{table}/fielddefinitions", auth=False)[1]}
    rc = fields("rent_contracts")
    want = ["name", "surname", "email", "phone", "asset_request", "birthdate", "id_card", "status", "last_update",
            "creation_date"]
    check("metadata Rent Contracts has all fields", all(f in rc for f in want),
          "missing " + str([f for f in want if f not in rc]) if not all(f in rc for f in want) else "")
    check("Rent Contracts: id_card is a document (media) field", rc.get("id_card", {}).get("type") == "media")
    check("Rent Contracts: status options CREATED,APPROUVED,REFUSED",
          rc.get("status", {}).get("options") == "CREATED,APPROUVED,REFUSED")
    wi = fields("rent_contracts_workflow_instances")
    want = ["workflow_instance_uuid", "start_date", "end_date", "final_status"]
    check("metadata Rent Contracts Workflow Instances has all fields", all(f in wi for f in want))
    check("Workflow Instances: final_status options APPROUVED,REFUSED",
          wi.get("final_status", {}).get("options") == "APPROUVED,REFUSED")
    check("contracts are not readable anonymously (personal data)",
          http("GET", f"{SNELLO}/api/rent_contracts", auth=False)[0] == 401)


# ---------------------------------------------------------------- one contract
def upload_id_card(tag):
    boundary = "----e2e" + uuidlib.uuid4().hex
    content = f"%PDF-1.4\n% dummy ID card {tag}\n".encode()
    parts = []
    for k, v in {"filename": f"idcard-{tag}.pdf", "mimeType": "application/pdf", "table_name": "rent_contracts",
                 "table_key": "pending"}.items():
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"idcard-{tag}.pdf\"\r\n"
                 f"Content-Type: application/pdf\r\n\r\n".encode() + content + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    s, d, _ = http("POST", f"{SNELLO}/api/documents", b"".join(parts),
                   headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    assert s == 200, f"upload failed {s} {d}"
    return d["uuid"], content


def decide(task_id, decision):
    """The reviewer's click: the human task's decide / undo (snello checks role, decision, task still open)."""
    if decision is None:
        return http("POST", f"{SNELLO}/api/humantasks/{task_id}/undo", {})
    return http("POST", f"{SNELLO}/api/humantasks/{task_id}/decide", {"decision": decision})


def task_of(wi):
    s, rows, _ = http("GET", f"{SNELLO}/api/humantasks?instance_id={wi}")
    return rows[0] if s == 200 and rows else None


def row(cuuid):
    r = sql("select c.status, c.creation_date, c.last_update, c.contract_document, w.workflow_instance_uuid, "
            f"w.start_date, w.end_date, w.final_status, w.decision from rent_contracts c left join "
            f"rent_contracts_workflow_instances w using(uuid) where uuid='{cuuid}'")
    keys = ["status", "creation_date", "last_update", "contract_document", "wi", "start_date", "end_date",
            "final_status", "decision"]
    return dict(zip(keys, r.split("|"))) if r else {}


def scenario(label, first, surname):
    tag = f"{label}-{RUN}"
    email = f"{first.lower()}.{surname.lower()}.{RUN}@example.com"
    doc, doc_bytes = upload_id_card(tag)
    s, c, _ = http("POST", f"{SNELLO}/api/rent_contracts", {
        "name": first, "surname": surname, "email": email, "phone": "+39 333 0000000",
        "asset_request": f"e2e {tag}: scooter for two weeks", "birthdate": "1991-04-12", "id_card": doc})
    if not check(f"[{label}] save Rent Contract", s == 200, str(s)):
        return
    cu = c["uuid"]
    log(f"[{label}] contract {cu} ({email})")

    # ---- post persist action
    r = wait_for(lambda: (lambda x: x if x.get("wi") else None)(row(cu)), 30)
    check(f"[{label}] post persist: status CREATED + creation_date + last_update",
          r.get("status") == "CREATED" and r.get("creation_date") and r.get("last_update"), str(r))
    m = wait_for(lambda: find_mail(email, "Your rent request", "we will evaluate your request. Be patient."), 20)
    check(f"[{label}] post persist: mail to {{rentContract.email}} 'we will evaluate your request. be patient'", m)
    text = LOG.read_text(errors="replace")
    att = re.search(rf"rent contract {cu}: \"we will evaluate\" mail sent to \S+ with attachment (\S+) \((\d+) bytes\)", text)
    check(f"[{label}] post persist: contract PDF generated and attached", att and int(att.group(2)) > 1000,
          att.group(0)[-60:] if att else "no attachment log line")
    s, pdf, hdr = http("GET", f"{SNELLO}/api/printtemplates/rent_contract/print?uuid={cu}", raw=True)
    check(f"[{label}] contract PDF renders with the user data", s == 200 and pdf[:5] == b"%PDF-", f"{s}, {len(pdf)} B")
    check(f"[{label}] post persist: workflow started, instance row uuid = rentcontract_uuid + workflow_instance_uuid + start_date",
          r.get("wi") and r.get("start_date"), f"instance {r.get('wi')}")
    wi = r.get("wi")

    # ---- workflow: reviewer mail + page
    # match on the applicant email (unique per run) in the body: the same names exist from earlier runs
    m = wait_for(lambda: find_mail(REVIEWER, f"{first} {surname}", email), 20)
    if not check(f"[{label}] workflow: mail to reviewer {REVIEWER}", m):
        return
    links = [l.replace("&amp;", "&") for l in re.findall(r'href="([^"]+)"', m["html"])]
    review = next((l for l in links if "rent-contract-review" in l), "")
    q = urllib.parse.parse_qs(urllib.parse.urlparse(review).query)
    task = wait_for(lambda: task_of(wi), 20) or {}
    tid = task.get("uuid")
    check(f"[{label}] workflow opened a human task for the contract (ACCEPT,REFUSE, Admin/Manager)",
          task.get("status") == "OPEN" and task.get("record_key") == cu and task.get("decisions") == "ACCEPT,REFUSE"
          and task.get("roles") == "Admin,Manager" and task.get("page_name") == "rent-contract-review", str(task)[:200])
    check(f"[{label}] review link = the task's page /p/rent-contract-review?task=<task>",
          urllib.parse.urlparse(review).path == "/p/rent-contract-review" and q.get("task") == [tid], review)
    stored = wait_for(lambda: sql(f"select review_url from rent_contracts_workflow_instances where uuid='{cu}'"), 20)
    stored_href = (re.findall(r'href="([^"]+)"', stored or "") or [""])[0].replace("&amp;", "&")
    check(f"[{label}] instance row review_url = the emailed review link (clickable in admin)",
          stored_href == review, stored_href or "empty")
    on_contract = wait_for(lambda: sql(f"select review_url from rent_contracts where uuid='{cu}'"), 20)
    contract_href = (re.findall(r'href="([^"]+)"', on_contract or "") or [""])[0].replace("&amp;", "&")
    check(f"[{label}] contract row review_url = the emailed review link (shown on the contract page)",
          contract_href == review, contract_href or "empty")
    s, page, _ = http("GET", review, auth=False, raw=True)
    page = page.decode() if isinstance(page, bytes) else page
    check(f"[{label}] review page served (workflow page + snello page client)",
          s == 200 and "snello-page.js" in page and "renderDecisions" in page, str(s))
    s2, ctx, _ = http("GET", f"{SNELLO}/api/humantasks/{tid}/context")
    check(f"[{label}] task context: the contract + its workflow position, for the page",
          s2 == 200 and ctx["record"]["uuid"] == cu and ctx.get("workflow", {}).get("instanceId") == wi, str(s2))
    s3, _, _ = http("GET", f"{SNELLO}/api/humantasks/{tid}/context", auth=False)
    check(f"[{label}] task context is not readable anonymously", s3 == 401, str(s3))
    check(f"[{label}] review page has no personal data without login", email not in page and surname not in page)
    idlink = next((l for l in links if f"/api/documents/{doc}/download" in l), None)
    s, got, _ = http("GET", idlink, auth=False, raw=True) if idlink else (0, b"", None)
    check(f"[{label}] workflow input has the S3 link: ID card downloadable from the mail", s == 200 and got == doc_bytes,
          idlink or "no ID card link")

    # ---- decision
    if label == "C":
        decide(tid, "ACCEPT")
        log("[C] ACCEPT, holding 26 s so a poll sees it, then UNDO")
        time.sleep(26)
        s, _, _ = decide(tid, None)
        time.sleep(40)
        r = row(cu)
        check("[C] UNDO: ACCEPT seen by the workflow then undone -> nothing applied, still open",
              s == 200 and r.get("status") == "CREATED" and not r.get("final_status"), str(r))
        final = "REFUSE"
    else:
        final = "ACCEPT" if label == "A" else "REFUSE"
    s, body, _ = decide(tid, "MAYBE")
    check(f"[{label}] a decision outside the task's list is refused", s == 400, str(s))
    s, _, _ = decide(tid, final)
    log(f"[{label}] {final} sent ({s})")
    expect = "APPROUVED" if final == "ACCEPT" else "REFUSED"
    r = wait_for(lambda: (lambda x: x if x.get("final_status") else None)(row(cu)), 90)
    path = "acceptance" if final == "ACCEPT" else "refusal"
    check(f"[{label}] {path} path: Rent Contract status {expect}", r.get("status") == expect, str(r))
    check(f"[{label}] {path} path: last_update updated on update", r.get("last_update", "") > r.get("creation_date", "z"),
          f"{r.get('creation_date')} -> {r.get('last_update')}")
    if final == "ACCEPT":
        m = wait_for(lambda: find_mail(email, "approved", f"Dear {first} {surname}"), 20)
        check(f"[{label}] acceptance path: mail 'Dear name surname your contract is approved ... one day' cc {REVIEWER}",
              m and REVIEWER in m["cc"] and "more info in one day" in m["html"], m and m["html"][:90])
        d = r.get("contract_document")
        meta = sql(f"select path, table_name, table_key, mimetype from documents where uuid='{d}'") if d else ""
        key = meta.split("|")[0] if meta else ""  # snello keeps the bucket name in the key: snello/rent_contracts/x.pdf
        head = subprocess.run(["docker", "run", "--rm", "--network", "host",
                               "-e", "AWS_ACCESS_KEY_ID=" + os.environ.get("MINIO_USER", "minio"),
                               "-e", "AWS_SECRET_ACCESS_KEY=" + os.environ.get("MINIO_PASSWORD", "minio123"),
                               "-e", "AWS_DEFAULT_REGION=us-east-1", "amazon/aws-cli:2.31.38",
                               "--endpoint-url", "http://localhost:9000", "s3api", "head-object",
                               "--bucket", "snello", "--key", key], capture_output=True, text=True) if key else None
        check(f"[{label}] acceptance path: contract stored in S3 (object in MinIO bucket snello, snello document linked)",
              head and head.returncode == 0 and f"|rent_contracts|{cu}|application/pdf" in meta,
              f"s3://snello/{key} " + (json.loads(head.stdout).get("ContentLength", "?").__str__() + " B" if head and head.returncode == 0 else (head.stderr.strip()[:120] if head else "no document")))
        s, got, _ = http("GET", f"{SNELLO}/api/documents/{d}/download", raw=True) if d else (0, b"", None)
        check(f"[{label}] acceptance path: stored contract downloadable via snello", s == 200 and got[:5] == b"%PDF-")
    else:
        m = wait_for(lambda: find_mail(email, "refused", f"Dear {first} {surname}"), 20)
        check(f"[{label}] refusal path: mail 'Dear name surname your contract is refused ... 01-1234567' cc {REVIEWER}",
              m and REVIEWER in m["cc"] and "01-1234567" in m["html"], m and m["html"][:90])
        check(f"[{label}] refusal path: no contract stored in S3", not r.get("contract_document"))
    check(f"[{label}] {path} path: Workflow Instance updated by PUT: final_status {expect} + end_date",
          r.get("final_status") == expect and r.get("end_date"), str(r))
    def completed():
        out = subprocess.run(["docker", "logs", "flow-runner"], capture_output=True, text=True).stdout
        return any(wi in line and "workflow.completed" in line for line in out.splitlines())
    done = wait_for(completed, 20)
    check(f"[{label}] END of workflow (workflow.completed)", done)
    t = task_of(wi) or {}
    check(f"[{label}] task CLOSED by the workflow with the decision {final}",
          t.get("status") == "CLOSED" and t.get("decision") == final and t.get("closed_at"), str(t)[:160])
    s, body, _ = decide(tid, "ACCEPT")
    check(f"[{label}] decision after the end is refused (no UNDO after close)", s == 409, f"{s} {str(body)[:80]}")


def main():
    log(f"e2e run {RUN}: snello {SNELLO}, runner {RUNNER}, log {LOG}")
    check_infra()
    with concurrent.futures.ThreadPoolExecutor(3) as ex:
        futures = [ex.submit(scenario, *a) for a in (("A", "Alice", "Accetta"), ("B", "Bruno", "Rifiuta"),
                                                      ("C", "Carla", "Ripensa"))]
        for f in futures:
            f.result()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed" + (": FAILED " + "; ".join(r[0] for r in failed) if failed else ""))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
