# snello-flows

Workflows ([Quarkus Flow](https://docs.quarkiverse.io/quarkus-flow/dev/), CNCF Serverless Workflow 1.0) that run next
to a snello instance, plus everything they need inside snello: Actions, workflow pages, print templates, setup scripts
and end-to-end tests. snello itself (human tasks, workflow pages, `/api/flow`) lives in snello-api; this repo only
uses it.

| Path | What |
|---|---|
| `workflows/` | Workflows the runner loads (local `compose.yml`; aruba-k8s renders them for the cluster) |
| `contracts/` | Rent contracts: snello Actions, review page, contract PDF template, `setup.py`, e2e and remote checks |
| `events/` | New-event notification: event page, `setup.py` |
| `bridge/` | flow-bridge: follows the runner's event log, answers "where is each instance" for the Workflow Monitor |
| `examples/` | Learning workflows (`hello-world`, `explore-*`) and their output sink; not loaded by the runner |
| `config/`, `data/`, `backups/` | Local only, git-ignored: runner secrets, instance store, backups |

## Local runner

`quay.io/quarkiverse/quarkus-flow-runner:1.3.0-minimal` via `compose.yml`: workflows from `workflows/` (ro), secrets
from `config/application.properties` (git-ignored; template below), instance state in `data/flowRunnerData.mv`
(MVStore, H2's storage engine; the `minimal` flavor is the only one without Postgres).

```bash
docker compose up -d                 # logs: docker compose logs -f flow-runner
./backup.sh                          # pause, copy data/flowRunnerData.mv to backups/ (keeps 14), unpause
```

Host network because snello-control listens on 127.0.0.1:8080 only. Security is off by default
(`SECURITY DISABLED` in the log): keep it bound to 127.0.0.1, or set `quarkus.flow.runner.security.type=API_KEY`.

## Endpoints (1.3.0)

| | |
|---|---|
| `GET /q/flow/definitions`, `GET /q/flow/definitions/{ns}/{name}/{version}` | loaded workflows (without version: 404) |
| `POST /q/flow/exec/{namespace}/{name}` | start (202 + instanceId); `.../sync` waits for the result |
| `GET /q/flow/instances` | running / waiting instances (completed ones are not listed) |
| `POST /q/flow/exec/{ns}/{name}/{instanceId}/suspend\|resume\|cancel` | |
| `GET /q/health/ready` | readiness |

The file watcher registers new files but not edits of a loaded file: `docker restart flow-runner`.
`document.name` must match `[a-zA-Z0-9-]` (no `_`). YAML anchors/aliases are not resolved: reuse retry policies
with `use.retries` + `retry: <name>`.

**Restart caveat (1.3.0-minimal):** an instance interrupted by a runner restart while in a `wait` stayed `PENDING`
(not resumed by the 30 s recovery scan, tested 2026-11-17). Avoid restarting while contracts are waiting for review,
or re-check `GET /q/flow/instances` afterwards.

## snello:new-event

Input `{"key": "<event uuid>"}` → `GET snello /api/events/{key}` → `POST snello /api/mail/send` (snello sends with its
own SMTP settings, `SNELLO_MAIL_*`; no mail credentials here). The mail call logs in with OAuth2 client credentials as
the Keycloak client `snello-flow`, whose service account has the realm role `Manager`. Locally it comes from
`snello-api/dev/keycloak/realm-snello.json` (secret `snello-flow-dev-secret`, dev only), so a recreated dev Keycloak
has it again; elsewhere create it by hand with its own secret.
Communication errors are retried (fetch 3×, mail 5×, exponential backoff); 4xx other than 429 fail at once.
Started by the snello Action `snello-api/dev/actions/events-new-event-flow.js`
(snello needs `snello.http.allow-private=true` + `snello.http.allowed-hosts=<runner host>`).

```properties
# config/application.properties
snello.apiUrl=http://127.0.0.1:8080
snello.publicUrl=http://localhost:8080   # links in mails; the cluster renders its own (https://api.sne.lol)
notify.to=<recipients, comma separated>
snelloAuth.clientId=snello-flow
snelloAuth.clientSecret=snello-flow-dev-secret   # local dev realm; another secret anywhere else
```

The token authority is a literal in the workflow (`use.authentications.snelloService.oauth2.authority`; the schema
rejects expressions there). It must be the same host snello trusts as issuer (`localhost`, not `127.0.0.1`, locally).

Flow 1.0 notes: in `catch.exceptWhen` the caught error is the input (`.status`); a `catch.as` variable is not bound.
`oidc` authentication posts to the authority itself (no discovery): use `oauth2` with `endpoints.token`.
In `export.as`, `.` is the current context and the task result is `$output` (the spec says `.` is the output).
A caught error still logs a `task.faulted` event: check `workflow.completed` / `workflow.faulted`, not task faults.

## contracts:contract-evaluation (rent contracts)

Requested as `contract_evaluation`; the DSL forbids `_` in names. Files: `workflows/contracts-contract-evaluation.yaml`,
snello side in `contracts/` (applied by `python3 -I contracts/setup.py`, idempotent: rerun after editing any file).

1. snello `POST /api/rent_contracts` → Action **rent-contracts-post-persist** (POST, async): status `CREATED` +
   timestamps, PDF from print template `rent_contract` mailed to the applicant ("we will evaluate your request. Be
   patient."), starts the workflow with `{contract, idCardUrl}`, creates `rent_contracts_workflow_instances`
   (uuid = contract uuid, `workflow_instance_uuid`, `start_date`).
2. Workflow opens a snello **human task** (`POST /api/humantasks`: record `rent_contracts/<uuid>`, decisions
   `ACCEPT,REFUSE`, roles `Admin,Manager`, page `rent-contract-review`) and mails `contracts.reviewer` its link
   `/p/rent-contract-review?task=<task>` + ID card link; the link is also stored as `review_url`.
3. Review page = snello **workflow page** `rent-contract-review` (`contracts/pages/rent-contract-review.html`, written
   by setup.py): Keycloak login, then applicant, request, documents (contract PDF with the reviewer's token), workflow
   progress and the decision bar (ACCEPT / REFUSE / UNDO), all through `window.snello` (see snello-api
   `docs/human-tasks.md`). Nothing personal is in the link or the HTML; snello checks role, decision and task state.
4. Workflow polls the task every 20 s. A decision counts only if `decision` + `decided_at` are unchanged 30 s later
   (the UNDO window), then it closes the task (no more UNDO; later decisions get 409) and:
   - ACCEPT: `PUT rent_contracts {status: APPROUVED}` (PRE MERGE action **rent-contracts-pre-merge** stores the
     contract PDF in snello documents/S3, `contract_document`), mail "approved" (cc reviewer), `PUT` instance
     `{final_status: APPROUVED}` (`end_date` stamped by snello).
   - REFUSE: `PUT {status: REFUSED}`, mail "refused … 01-1234567" (cc reviewer), `PUT {final_status: REFUSED}`.

Why polling: the runner flavors that store in H2/MVStore (`minimal`) cannot receive events, so a `listen` task can
never be woken; events need the `messaging` flavor (Kafka + Postgres). Polling also makes UNDO real.

```properties
contracts.reviewer=fiorenzino@gmail.com
contracts.cc=fiorenzino@gmail.com
contracts.phone=01-1234567
```

End-to-end test (about 3 min, 88 checks, one per requirement; needs snello started with `SNELLO_MAIL_MOCK=true`,
mails are read from its log): contracts A = ACCEPT, B = REFUSE, C = ACCEPT → UNDO → REFUSE, in parallel.

```bash
SNELLO_LOG=<snello quarkus:dev output file> python3 -I contracts/e2e_test.py
```

snello needs `snello.http.allow-private=true` + `snello.http.allowed-hosts=127.0.0.1` (action → runner), and
`actions.body` as text (`snello-api/dev/db/actions-body-text-migration.sql`; the post-persist script is > 1000 chars).
Per environment change: the Keycloak authority in the workflow (aruba-k8s renders it), `SNELLO_FLOW_URL` /
`SNELLO_PUBLIC_URL` env of snello; pages log in with `snello.pages.keycloak.client-id` (default snello-admin).

### Remote (api.sne.lol, cluster aruba-k8s)

The runner runs in namespace `snello` (`aruba-k8s/namespaces/snello/flow-runner`, ClusterIP + NetworkPolicy: only
snello-aoi reaches it); snello-aoi has `SNELLO_FLOW_URL=http://flow-runner:8090`, `SNELLO_HTTP_ALLOW_PRIVATE=true`,
`SNELLO_HTTP_ALLOWED_HOSTS=flow-runner`. Snello there is H2 + filesystem documents (`/data/files`), not S3.

```bash
# after editing a workflow: render it for the cluster (Keycloak authority) and deploy
aruba-k8s/scripts/flow-runner-workflows.sh && aruba-k8s/scripts/deploy-project.sh snello flow-runner
# runner config (client secret of snello-flow in alphasnello, reviewer); then restart the runner
FLOW_CLIENT_SECRET=... CONTRACTS_REVIEWER=... aruba-k8s/scripts/flow-runner-secret.sh
# snello side (password asked at the prompt, or SNELLO_TOKEN=<Admin access token>)
SNELLO_URL=https://api.sne.lol \
KEYCLOAK_TOKEN_URL=https://accounts.kayak.love/realms/alphasnello/protocol/openid-connect/token \
REVIEW_KEYCLOAK_URL=https://accounts.kayak.love REVIEW_KEYCLOAK_REALM=alphasnello REVIEW_KEYCLOAK_CLIENT=snello-admin \
SNELLO_ADMIN_USER=<you> python3 -I contracts/setup.py
# API-only check (real mails: applicants are APPLICANT_EMAIL+A-<run> / +B-<run>)
APPLICANT_EMAIL=<your inbox> SNELLO_ADMIN_USER=<you> python3 -I contracts/remote_check.py
```

## Monitoring: flow-bridge + admin Workflow Monitor

The runner API lists only active instances and not their current task. Its structured event log does, so the runner
also writes it to `data/flow-events.log` (`quarkus.log.handler.file."FLOW_FILE"` in `config/application.properties`)
and **flow-bridge** (`bridge/flow_bridge.py`, Python stdlib, compose service `flow-bridge`, port 8091) follows it:
`GET /instances?namespace=&name=&version=&status=` and `GET /instances/{id}` (status, current task, every task position
with its last event and run count, error). Payloads are not kept. State is rebuilt from the file + rotated backups.

snello forwards to both (`/api/flow/*`, Admin/Manager; `SNELLO_FLOW_URL`, `SNELLO_FLOW_BRIDGE_URL`), and snello admin's
**Workflow Monitor** (`#/flowmonitor`) draws the workflow (Mermaid, one box per top-level task, `switch`/`then` arrows)
and colours where the selected instance is: done / current / faulted, `×N` for polling loops. Suspend / resume / cancel
from the instance list. On the cluster flow-bridge is a second container in the runner pod (same PVC, read-only).

## Workflows from the snello admin (deploy through flow-bridge)

snello admin **Workflows** (`#/workflowdefinitions`): a YAML textarea with a live check and diagram, saved in snello's
`workflowdefinitions` table (`/api/workflowdefinitions`, Admin/Manager). **Deploy** = snello -> flow-bridge
`PUT /workflows/{ns}/{name}/{version}` (header `X-Bridge-Token`) -> the bridge writes `workflows/ui-{ns}-{name}-{version}.yaml`
and waits until the runner has registered it (max 20 s). The runner 1.3.0 has no deploy API and only logs a WARN for a bad
file, so it also writes its file-watcher log to `data/flow-watcher.log` (`WATCH_FILE` handler, below) and the bridge
returns that error (422) after removing the file again.

Runner behaviour this relies on (tested 2026-11-18 with 1.3.0-minimal):
- new files in the **top** folder load within 5 s; files added to a subfolder later are not seen (only at start);
- an edited, already loaded file is never reloaded -> every deployed version is immutable, a change is a new version;
- several versions of one workflow live side by side; `POST /q/flow/exec/{ns}/{name}` runs the newest,
  `/{ns}/{name}/{version}` a given one; running instances keep their version;
- a deleted file stays loaded until the runner restarts.

`ui-*.yaml` are git-ignored here: their source is snello. The git-managed workflows (`contracts-*`, `snello-*`) stay in git;
deploying one of their ids from the admin is refused (already loaded). The bridge enables writes only with both
`WORKFLOWS_DIR` and `BRIDGE_WRITE_TOKEN` (compose: dev token `snello-bridge-dev-token`, snello `%dev` has the same).

```properties
# config/application.properties: the runner's file watcher to a file, for flow-bridge
quarkus.log.handler.file."WATCH_FILE".enabled=true
quarkus.log.handler.file."WATCH_FILE".path=/deployments/data/flow-watcher.log
quarkus.log.handler.file."WATCH_FILE".format=%d{yyyy-MM-dd'T'HH:mm:ss.SSS} %-5p %s%n
quarkus.log.handler.file."WATCH_FILE".rotation.max-file-size=2M
quarkus.log.handler.file."WATCH_FILE".rotation.max-backup-index=1
quarkus.log.category."io.quarkiverse.flow.runner.WorkflowFileWatcher".handlers=WATCH_FILE
```

## examples: explore:* (learning workflows, read-only)

In `examples/workflows/`, not loaded by default: copy one into `workflows/` to run it. The 1.0.0 runner had no API
to read a workflow's output, so each one ends with a POST of its output to a local sink that prints it:
`python3 -I examples/flow_sink.py` (any server on `127.0.0.1:8099` that accepts JSON and answers `{}`).
They only GET public snello endpoints, no auth.

| Workflow | Feature | Input |
|---|---|---|
| `explore:kayak-stats` | `call: http` + `set` with jq aggregation (`group_by`, `max_by`) | `{}` |
| `explore:kayak-advice` | `switch` + `then` jumps, branches converging on one task | `{"key": "<kayak uuid>"}` |
| `explore:brand-census` | `for` loop, one http call per item, accumulating in `$context` via `export.as` | `{}` |
| `explore:safe-lookup` | `try`/`catch` filtered by `status: 404`, fallback via `$workflow.input` | `{"key": "<kayak uuid>"}` |

```bash
curl -s -X POST -H 'Content-Type: application/json' -d '{"key":"510"}' \
  http://127.0.0.1:8090/q/flow/exec/explore/kayak-advice
```
