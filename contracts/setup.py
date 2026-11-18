#!/usr/bin/env python3
"""Creates / updates in snello everything the rent contracts flow needs. Idempotent: run it again after editing a file.

  metadata rent_contracts, rent_contracts_workflow_instances, web_pages (+ fields, tables)
  actions  rent-contracts-post-persist, rent-contracts-pre-merge, rent-contracts-workflow-instances-pre-merge
  print template rent_contract (pdf), workflow page rent-contract-review (snello /p/rent-contract-review?task=...)

Env: SNELLO_URL (http://localhost:8080), KEYCLOAK_TOKEN_URL (local realm snello), SNELLO_ADMIN_USER / SNELLO_ADMIN_PASSWORD
(admin / admin, Admin role: table creation needs it), KEYCLOAK_CLIENT (snello-api, public, direct grants).
SNELLO_TOKEN: an Admin access token to use instead of the password login (e.g. when the realm has no direct-grant client).
Off localhost the password is asked at the prompt when SNELLO_ADMIN_PASSWORD is not set (never the dev default).
Usage: python3 -I contracts/setup.py
Remote: SNELLO_URL=https://api.sne.lol KEYCLOAK_TOKEN_URL=https://accounts.kayak.love/realms/alphasnello/protocol/openid-connect/token \
        SNELLO_ADMIN_USER=<you> python3 -I contracts/setup.py
"""
import getpass
import json
import os
import pathlib
import re
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
SNELLO = os.environ.get("SNELLO_URL", "http://localhost:8080").rstrip("/")
TOKEN_URL = os.environ.get("KEYCLOAK_TOKEN_URL", "http://localhost:8180/realms/snello/protocol/openid-connect/token")
LOCAL = urllib.parse.urlparse(SNELLO).hostname in ("localhost", "127.0.0.1")


def log(msg):
    print(msg, flush=True)


def token():
    if os.environ.get("SNELLO_TOKEN"):
        log("login: SNELLO_TOKEN from env")
        return os.environ["SNELLO_TOKEN"]
    user = os.environ.get("SNELLO_ADMIN_USER", "admin")
    password = os.environ.get("SNELLO_ADMIN_PASSWORD")
    if password is None:
        # the dev default only for a local snello: a remote one gets a prompt, never admin/admin
        password = "admin" if LOCAL else getpass.getpass(f"password of {user} at {TOKEN_URL}: ")
    log(f"login: password grant as {user} via client {os.environ.get('KEYCLOAK_CLIENT', 'snello-api')} at {TOKEN_URL}")
    body = urllib.parse.urlencode({
        "grant_type": "password", "client_id": os.environ.get("KEYCLOAK_CLIENT", "snello-api"),
        "username": user, "password": password}).encode()
    with urllib.request.urlopen(urllib.request.Request(TOKEN_URL, body)) as r:
        return json.load(r)["access_token"]


TOKEN = token()


def api(method, path, body=None):
    req = urllib.request.Request(SNELLO + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json",
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            text = r.read().decode()
            return json.loads(text) if text.strip() else None
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code} {e.read().decode()[:500]}")


def find(resource, **filters):
    qs = urllib.parse.urlencode({**filters, "_limit": 100})
    return api("GET", f"/api/{resource}?{qs}") or []


def upsert(resource, match, row):
    """POST when no row matches `match` (exact), else PUT the row onto the existing uuid."""
    hits = [h for h in find(resource, **match) if all(str(h.get(k)) == str(v) for k, v in match.items())]
    if hits:
        uuid = hits[0]["uuid"]
        api("PUT", f"/api/{resource}/{uuid}", {**hits[0], **row, "uuid": uuid})
        log(f"  {resource} {match} updated ({uuid})")
        return {**hits[0], **row}
    created = api("POST", f"/api/{resource}", row)
    log(f"  {resource} {match} created ({created.get('uuid')})")
    return created


def f(name, type_, label=None, **extra):
    return {"name": name, "label": label or name.replace("_", " ").capitalize(), "type": type_, **extra}


METADATA = [
    ({"table_name": "rent_contracts", "description": "Rent contract requests (evaluated by contracts:contract-evaluation)",
      "icon": "fa fa-file-signature", "table_key": "uuid", "table_key_type": "uuid", "metadata_group": "data",
      "api_protected": True, "username_field": "created_by", "order_by": "creation_date desc",
      "select_fields": "uuid,name,surname,email,status,creation_date", "search_fields": "name,surname,email,status"},
     # Order = the form order in the admin (it sorts by order_num). The admin has no per-field "hidden" or
     # "read only" (input_disabled is unused there), so the automatic fields sit at the bottom, say so in their
     # label, and the PRE MERGE action ignores anything typed into them.
     [f("name", "input", input_type="text", mandatory=True, show_in_list=True, searchable=True, search_condition="containss"),
      f("surname", "input", input_type="text", mandatory=True, show_in_list=True, searchable=True, search_condition="containss"),
      f("email", "input", input_type="email", mandatory=True, show_in_list=True, searchable=True, search_condition="containss"),
      f("phone", "input", input_type="text"),
      f("birthdate", "date"),
      f("asset_request", "textarea", "Asset request"),
      # media fields can only be filled after the first save (admin limitation): the label says so
      f("id_card", "media", "ID card (upload after saving)"),
      f("status", "select", "Status (set by the evaluation)", options="CREATED,APPROUVED,REFUSED", show_in_list=True,
        searchable=True, search_condition=""),
      # copied from the workflow instance row by its PRE MERGE action (the workflow writes it there)
      f("review_url", "tinymce", "Review page (link)", input_disabled=True),
      f("creation_date", "datetime", "Creation date (automatic)", input_disabled=True, show_in_list=True),
      f("last_update", "datetime", "Last update (automatic)", input_disabled=True),
      f("contract_document", "media", "Approved contract (automatic)", input_disabled=True),
      f("created_by", "input", "Created by (automatic)", input_type="text", input_disabled=True)]),
    ({"table_name": "rent_contracts_workflow_instances",
      "description": "One contract-evaluation run per rent contract (uuid = rent contract uuid)",
      "icon": "fa fa-diagram-project", "table_key": "uuid", "table_key_type": "uuid", "metadata_group": "data",
      "api_protected": True, "username_field": "created_by", "order_by": "start_date desc",
      "select_fields": "uuid,workflow_instance_uuid,start_date,end_date,final_status,decision"},
     [f("workflow_instance_uuid", "input", "Workflow instance", input_type="text", show_in_list=True),
      # the review page link the workflow emailed; an <a> tag, because the admin renders HTML (clickable) only
      # for rich-text fields. Written by the workflow (task storeReviewUrl).
      f("review_url", "tinymce", "Review page (link)", input_disabled=True),
      f("start_date", "datetime", show_in_list=True),
      f("end_date", "datetime", show_in_list=True),
      f("final_status", "select", options="APPROUVED,REFUSED", show_in_list=True),
      # written by the review page (ACCEPT / REFUSE / UNDO = empty); decision_date by the PRE MERGE action
      f("decision", "select", options="ACCEPT,REFUSE"),
      f("decision_date", "datetime", "Decision date (automatic)", input_disabled=True),
      f("created_by", "input", "Created by (automatic)", input_type="text", input_disabled=True)]),
    ({"table_name": "web_pages", "description": "Qute HTML pages served at /api/p/web_pages/{name} (field body)",
      "icon": "fa fa-file-code", "table_key": "name", "table_key_type": "userdefined", "metadata_group": "settings",
      "api_protected": False, "select_fields": "name,description"},
     [f("name", "input", input_type="text", mandatory=True, table_key=True, show_in_list=True),
      f("description", "input", input_type="text", show_in_list=True),
      f("body", "monaco", "Body (Qute)")]),
]

ACTIONS = [
    ("rent-contracts-post-persist", "rent_contracts", "PERSIST", "POST",
     "PDF + 'we will evaluate' mail, starts contracts:contract-evaluation, creates the workflow instance row"),
    ("rent-contracts-pre-merge", "rent_contracts", "MERGE", "PRE",
     "last_update; on APPROUVED stores the contract PDF in documents (S3)"),
    ("rent-contracts-workflow-instances-pre-merge", "rent_contracts_workflow_instances", "MERGE", "PRE",
     "decision_date / end_date stamps, refuses decision changes once closed"),
]


def main():
    log(f"snello {SNELLO} ({'local' if LOCAL else 'remote'})")
    for meta, fields in METADATA:
        table = meta["table_name"]
        log(f"metadata {table}")
        m = upsert("metadatas", {"table_name": table}, meta)
        existing = {x["name"] for x in find("fielddefinitions", metadata_name=table)}
        for i, fd in enumerate(fields, 1):
            row = {"metadata_uuid": m["uuid"], "metadata_name": table, "view_index": i, "order_num": i, **fd}
            if fd.get("searchable"):
                # the admin search box sends ?<search_field_name>=<value>; without it the request is ?null=<value>
                # (WHERE null = ?, nothing found). Same rule as the admin's field editor: <name>_<condition>, or
                # <name> for "equals". containss = lower(col) LIKE lower(%value%): case-insensitive, any database.
                cond = fd.get("search_condition") or ""
                row["search_field_name"] = f"{fd['name']}_{cond}" if cond else fd["name"]
            if fd["name"] in existing:
                upsert("fielddefinitions", {"metadata_name": table, "name": fd["name"]}, row)
            else:
                api("POST", "/api/fielddefinitions", row)
                log(f"  field {fd['name']} ({fd['type']}) created")
        current = api("GET", f"/api/metadatas/{m['uuid']}")
        if not current.get("created"):
            api("POST", f"/api/metadatas/{m['uuid']}/create")
            log(f"  table {table} created")

    log("actions")
    for name, table, condition, phase, desc in ACTIONS:
        body = (HERE / "actions" / f"{name}.js").read_text()
        upsert("actions", {"name": name}, {"name": name, "description": desc, "metadata_name": table,
                                           "condition": condition, "phase": phase, "body": body})

    log("print template rent_contract")
    upsert("printtemplates", {"name": "rent_contract"}, {
        "name": "rent_contract", "export_type": "pdf", "api_protected": True, "username_field": "created_by",
        "sql_query": "SELECT uuid, name, surname, email, phone, asset_request, birthdate, id_card, created_by, "
                     "to_char(creation_date, 'YYYY-MM-DD HH24:MI') AS creation_date FROM rent_contracts",
        "html_body": (HERE / "print" / "rent_contract.html").read_text()})

    log("workflow page rent-contract-review")
    page = {"name": "rent-contract-review", "title": "Rent contract review",
            "description": "Reviewer page of the contracts:contract-evaluation human task",
            "workflow_namespace": "contracts", "workflow_name": "contract-evaluation", "roles": "Admin,Manager",
            "body": (HERE / "pages" / "rent-contract-review.html").read_text()}
    upsert("workflowpages", {"name": "rent-contract-review"}, page)
    log(f"review page: {SNELLO}/p/rent-contract-review?task=<human task uuid> (the workflow mails the link)")


if __name__ == "__main__":
    main()
