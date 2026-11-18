#!/usr/bin/env python3
"""Rent contracts flow against a deployed snello, through its HTTP API only (no docker, no database, no mail log).

Two contracts in parallel: A = ACCEPT, B = REFUSE. Every check is something the API shows: status, timestamps, instance
row, review link, review page, stored contract PDF, END (final_status + end_date), no decision after the end.
Mails are REAL on a deployed snello: the applicant mails go to APPLICANT_EMAIL (+A / +B tags), the reviewer and cc
mails to whatever contracts.reviewer / contracts.cc the runner has. Check those inboxes by hand; this script cannot.

Env: SNELLO_URL (https://api.sne.lol), KEYCLOAK_TOKEN_URL (https://accounts.kayak.love/realms/alphasnello/...),
KEYCLOAK_CLIENT (a client with direct grants), SNELLO_ADMIN_USER (password asked at the prompt) or SNELLO_TOKEN,
APPLICANT_EMAIL (required: an inbox you read, e.g. you@gmail.com -> you+A-<run>@gmail.com).
Usage: APPLICANT_EMAIL=you@gmail.com SNELLO_ADMIN_USER=you python3 -I contracts/remote_check.py     (about 2 minutes)
"""
import concurrent.futures
import getpass
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid as uuidlib

SNELLO = os.environ.get("SNELLO_URL", "https://api.sne.lol").rstrip("/")
TOKEN_URL = os.environ.get("KEYCLOAK_TOKEN_URL",
                           "https://accounts.kayak.love/realms/alphasnello/protocol/openid-connect/token")
APPLICANT = os.environ["APPLICANT_EMAIL"]
RUN = uuidlib.uuid4().hex[:6]

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
    if os.environ.get("SNELLO_TOKEN"):
        log("login: SNELLO_TOKEN from env")
        return os.environ["SNELLO_TOKEN"]
    user = os.environ["SNELLO_ADMIN_USER"]
    client = os.environ.get("KEYCLOAK_CLIENT", "snello-api")
    password = os.environ.get("SNELLO_ADMIN_PASSWORD") or getpass.getpass(f"password of {user} at {TOKEN_URL}: ")
    log(f"login: password grant as {user} via client {client}")
    body = urllib.parse.urlencode({"grant_type": "password", "client_id": client, "username": user,
                                   "password": password}).encode()
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
        with urllib.request.urlopen(urllib.request.Request(url, data, h, method=method), timeout=30) as r:
            b = r.read()
            return r.status, (b if raw else (json.loads(b) if b.strip() else None)), r.headers
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace"), e.headers


def wait_for(fn, timeout, every=3):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(every)
    return fn()


def tagged(label):
    local, _, domain = APPLICANT.partition("@")
    return f"{local}+{label}-{RUN}@{domain}"


def upload_id_card(tag):
    boundary = "----rc" + uuidlib.uuid4().hex
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
    if not check(f"[{tag}] upload ID card document", s == 200, str(s)):
        return None, content
    return d["uuid"], content


def contract(cu):
    s, c, _ = http("GET", f"{SNELLO}/api/rent_contracts/{cu}")
    return c if s == 200 else {}


def instance(cu):
    s, w, _ = http("GET", f"{SNELLO}/api/rent_contracts_workflow_instances/{cu}")
    return w if s == 200 else {}


def href(html):
    return (re.findall(r'href="([^"]+)"', html or "") or [""])[0].replace("&amp;", "&")


def check_setup():
    s, _, _ = http("GET", f"{SNELLO}/api/rent_contracts", auth=False)
    check("contracts are not readable anonymously (personal data)", s == 401, str(s))
    for table in ("rent_contracts", "rent_contracts_workflow_instances", "humantasks", "workflowpages"):
        s, _, _ = http("GET", f"{SNELLO}/api/{table}")
        check(f"table {table} exists", s == 200, str(s))
    s, page, _ = http("GET", f"{SNELLO}/p/rent-contract-review", auth=False, raw=True)
    page = page.decode(errors="replace") if isinstance(page, bytes) else page
    kc = re.search(r"window.SNELLO_PAGE = (\{.*?\});", page or "")
    check("review page served, with this environment's Keycloak (not localhost)",
          s == 200 and kc and "localhost" not in kc.group(1), kc.group(1) if kc else str(s))


def scenario(label, first, surname, decision):
    tag = f"{label}-{RUN}"
    email = tagged(label)
    doc, doc_bytes = upload_id_card(tag)
    s, c, _ = http("POST", f"{SNELLO}/api/rent_contracts", {
        "name": first, "surname": surname, "email": email, "phone": "+39 333 0000000",
        "asset_request": f"remote check {tag}", "birthdate": "1991-04-12", "id_card": doc})
    if not check(f"[{label}] save Rent Contract", s == 200, f"{s} {str(c)[:200] if s != 200 else ''}"):
        return
    cu = c["uuid"]
    log(f"[{label}] contract {cu} ({email})")

    w = wait_for(lambda: (lambda x: x if x.get("workflow_instance_uuid") else None)(instance(cu)), 45)
    r = contract(cu)
    check(f"[{label}] post persist: status CREATED + creation_date + last_update",
          r.get("status") == "CREATED" and r.get("creation_date") and r.get("last_update"),
          f"{r.get('status')} {r.get('creation_date')}")
    if not check(f"[{label}] post persist: workflow started, instance row with workflow_instance_uuid + start_date",
                 w.get("workflow_instance_uuid") and w.get("start_date"), str(w)[:200]):
        return
    wi = w["workflow_instance_uuid"]
    s, pdf, _ = http("GET", f"{SNELLO}/api/printtemplates/rent_contract/print?uuid={cu}", raw=True)
    check(f"[{label}] contract PDF renders", s == 200 and pdf[:5] == b"%PDF-", f"{s}, {len(pdf or b'')} B")

    w = wait_for(lambda: (lambda x: x if x.get("review_url") else None)(instance(cu)), 60)
    review = href(w.get("review_url"))
    q = urllib.parse.parse_qs(urllib.parse.urlparse(review).query)
    s, tasks, _ = http("GET", f"{SNELLO}/api/humantasks?instance_id={wi}")
    task = tasks[0] if s == 200 and tasks else {}
    tid = task.get("uuid")
    check(f"[{label}] workflow opened a human task (ACCEPT,REFUSE) for the contract",
          task.get("status") == "OPEN" and task.get("record_key") == cu and task.get("decisions") == "ACCEPT,REFUSE",
          str(task)[:160])
    check(f"[{label}] review link (mailed, stored on the instance row) = the task page",
          urllib.parse.urlparse(review).path == "/p/rent-contract-review" and q.get("task") == [tid], review or "empty")
    check(f"[{label}] review link is public (not localhost)", review.startswith(SNELLO), review)
    check(f"[{label}] contract row review_url = the same link", href(contract(cu).get("review_url")) == review)
    s, page, _ = http("GET", review, auth=False, raw=True) if review else (0, b"", None)
    page = page.decode(errors="replace") if isinstance(page, bytes) else str(page)
    check(f"[{label}] review page served (workflow page + snello page client)",
          s == 200 and "snello-page.js" in page and "renderDecisions" in page, str(s))
    check(f"[{label}] review page has no personal data without login", email not in page and surname not in page)
    if doc:
        s, got, _ = http("GET", f"{SNELLO}/api/documents/{doc}/download", raw=True)
        check(f"[{label}] ID card downloadable", s == 200 and got == doc_bytes, str(s))

    s, _, _ = http("POST", f"{SNELLO}/api/humantasks/{tid}/decide", {"decision": decision})
    log(f"[{label}] {decision} sent ({s}); the workflow applies it after its next poll + 30 s grace")
    expect = "APPROUVED" if decision == "ACCEPT" else "REFUSED"
    w = wait_for(lambda: (lambda x: x if x.get("final_status") else None)(instance(cu)), 120)
    r = contract(cu)
    path = "acceptance" if decision == "ACCEPT" else "refusal"
    check(f"[{label}] {path} path: Rent Contract status {expect}", r.get("status") == expect, str(r.get("status")))
    check(f"[{label}] {path} path: instance final_status {expect} + end_date (END)",
          w.get("final_status") == expect and w.get("end_date"), f"{w.get('final_status')} {w.get('end_date')}")
    d = r.get("contract_document")
    if decision == "ACCEPT":
        s, got, _ = http("GET", f"{SNELLO}/api/documents/{d}/download", raw=True) if d else (0, b"", None)
        check(f"[{label}] acceptance path: contract PDF stored (snello documents) and downloadable",
              s == 200 and got[:5] == b"%PDF-", f"document {d}, {s}")
    else:
        check(f"[{label}] refusal path: no contract stored", not d, str(d))
    s, body, _ = http("POST", f"{SNELLO}/api/humantasks/{tid}/decide", {"decision": "ACCEPT"})
    check(f"[{label}] decision after the end is refused (task CLOSED)", s == 409, f"{s} {str(body)[:80]}")
    log(f"[{label}] mails to check by hand: 'we will evaluate' + '{path}' to {email}; reviewer mail with the link")


def main():
    log(f"remote check {RUN}: snello {SNELLO}, applicants {tagged('X')}")
    check_setup()
    with concurrent.futures.ThreadPoolExecutor(2) as ex:
        for f in [ex.submit(scenario, "A", "Alice", "Accetta", "ACCEPT"),
                  ex.submit(scenario, "B", "Bruno", "Rifiuta", "REFUSE")]:
            f.result()
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed"
          + (": FAILED " + "; ".join(r[0] for r in failed) if failed else ""))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
