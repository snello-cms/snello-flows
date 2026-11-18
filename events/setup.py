#!/usr/bin/env python3
"""Writes the workflow page new-event (events/pages/new-event.html) into snello, the page the snello:new-event mail
links to: {snello.publicUrl}/p/new-event?key={event uuid}. Idempotent (upsert on the page name).

The workflow itself is started by the events POST PERSIST Action (snello-api dev/actions/events-new-event-flow.js);
this script does not touch actions.

Env: SNELLO_URL (http://localhost:8080), KEYCLOAK_TOKEN_URL (local dev realm), KEYCLOAK_CLIENT (snello-api),
SNELLO_ADMIN_USER (admin) + SNELLO_ADMIN_PASSWORD (prompted off localhost) or SNELLO_TOKEN.
Usage: python3 -I events/setup.py
"""
import getpass
import json
import os
import pathlib
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
SNELLO = os.environ.get("SNELLO_URL", "http://localhost:8080").rstrip("/")
TOKEN_URL = os.environ.get("KEYCLOAK_TOKEN_URL", "http://localhost:8180/realms/snello/protocol/openid-connect/token")
LOCAL = urllib.parse.urlparse(SNELLO).hostname in ("localhost", "127.0.0.1")
PAGE = "new-event"


def log(msg):
    print(msg, flush=True)


def token():
    if os.environ.get("SNELLO_TOKEN"):
        log("login: SNELLO_TOKEN from env")
        return os.environ["SNELLO_TOKEN"]
    user = os.environ.get("SNELLO_ADMIN_USER", "admin")
    client = os.environ.get("KEYCLOAK_CLIENT", "snello-api")
    password = os.environ.get("SNELLO_ADMIN_PASSWORD")
    if password is None:
        # the dev default only for a local snello: a remote one gets a prompt
        password = "admin" if LOCAL else getpass.getpass(f"password of {user} at {TOKEN_URL}: ")
    log(f"login: password grant as {user} via client {client} at {TOKEN_URL}")
    body = urllib.parse.urlencode({"grant_type": "password", "client_id": client, "username": user,
                                   "password": password}).encode()
    with urllib.request.urlopen(urllib.request.Request(TOKEN_URL, body)) as r:
        return json.load(r)["access_token"]


def api(tok, method, path, body=None):
    req = urllib.request.Request(SNELLO + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json",
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            text = r.read().decode()
            return json.loads(text) if text.strip() else None
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code} {e.read().decode()[:500]}")


def main():
    log(f"snello {SNELLO}: workflow page {PAGE}")
    tok = token()
    row = {"name": PAGE, "title": "New event",
           "description": "Read-only event page linked from the snello:new-event mail (?key=<event uuid>)",
           "workflow_namespace": "snello", "workflow_name": "new-event", "roles": "Admin,Manager",
           "body": (HERE / "pages" / f"{PAGE}.html").read_text()}
    hits = [h for h in api(tok, "GET", f"/api/workflowpages?name={PAGE}&_limit=10") or [] if h.get("name") == PAGE]
    if hits:
        uuid = hits[0]["uuid"]
        api(tok, "PUT", f"/api/workflowpages/{uuid}", {**hits[0], **row, "uuid": uuid})
        log(f"  workflowpages {PAGE} updated ({uuid})")
    else:
        created = api(tok, "POST", "/api/workflowpages", row)
        log(f"  workflowpages {PAGE} created ({created.get('uuid')})")
    log(f"page: {SNELLO}/p/{PAGE}?key=<event uuid> (the snello:new-event mail links there)")


if __name__ == "__main__":
    main()
