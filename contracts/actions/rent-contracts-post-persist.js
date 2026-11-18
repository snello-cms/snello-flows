// Action rent-contracts-post-persist — metadata_name: rent_contracts, condition: PERSIST, phase: POST (async).
// 1. stamps status CREATED + creation_date/last_update (one phase per table+condition: no PRE PERSIST possible)
// 2. renders the contract PDF (print template rent_contract) and mails it to the applicant: "we will evaluate"
// 3. starts the Quarkus Flow workflow contracts:contract-evaluation with the contract + the ID card link
// 4. creates the rent_contracts_workflow_instances row (uuid = contract uuid) holding the flow instance id
// Env: SNELLO_FLOW_URL (runner, default http://127.0.0.1:8090), SNELLO_PUBLIC_URL (links, default http://localhost:8080).
// snello needs snello.http.allow-private=true (+ snello.http.allowed-hosts=<runner host>) to reach the runner.
var env = java.lang.System.getenv();
var FLOW = (env.get('SNELLO_FLOW_URL') || 'http://127.0.0.1:8090').replace(/\/+$/, '');
var PUBLIC = (env.get('SNELLO_PUBLIC_URL') || 'http://localhost:8080').replace(/\/+$/, '');
var uuid = String(key);
log.infof('rent contract %s persisted: email=%s id_card=%s', uuid, map.get('email'), map.get('id_card'));

function str(v) { return v == null ? null : String(v); }
function esc(v) { return v == null ? '' : String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

// 1. server-side stamps (the API response of the POST does not show them: this runs after it)
if (!db.query('UPDATE rent_contracts SET status = ?, creation_date = now(), last_update = now() WHERE uuid = ?',
        java.util.List.of('CREATED', uuid))) {
    log.errorf('rent contract %s: status/timestamps update failed (see EXECUTE QUERY error above)', uuid);
}

// 2. contract PDF to the applicant
var pdf = prints.print('rent_contract', { uuid: uuid }, null);
log.infof('rent contract %s: pdf %s bytes, %s rows', uuid, pdf.bytes().length, pdf.rows());
var Attachment = Java.type('io.quarkus.mailer.Attachment'); // 'io' is not a Nashorn top-level package
var att = new Attachment('rent-contract-' + uuid + '.pdf', pdf.bytes(), 'application/pdf');
mail.send('Your rent request',
    'Dear ' + esc(map.get('name')) + ' ' + esc(map.get('surname')) + ',<br><br>'
    + 'we received your request: we will evaluate your request. Be patient.<br><br>'
    + 'Attached the contract with the data you sent us.',
    java.util.List.of(str(map.get('email'))), null, null, java.util.List.of(att));
log.infof('rent contract %s: "we will evaluate" mail sent to %s with attachment %s (%s bytes)', uuid, map.get('email'),
    'rent-contract-' + uuid + '.pdf', pdf.bytes().length);

// 3. start the workflow: plain JS copy of the row (JSON.stringify does not serialise a Java Map)
var contract = { uuid: uuid };
['name', 'surname', 'email', 'phone', 'asset_request', 'birthdate', 'id_card'].forEach(function (f) {
    contract[f] = str(map.get(f));
});
contract.status = 'CREATED';
var idCard = contract.id_card ? PUBLIC + '/api/documents/' + encodeURIComponent(contract.id_card) + '/download' : null;
var url = FLOW + '/q/flow/exec/contracts/contract-evaluation';
log.infof('rent contract %s: starting %s (idCardUrl=%s)', uuid, url, idCard);
var r = call(POST, url, JSON.stringify({ contract: contract, idCardUrl: idCard }));
if (!r.ok()) {
    throw 'rent contract ' + uuid + ': flow start failed status=' + r.status() + ' error=' + r.error() + ' body=' + r.body();
}
var instanceId = String(r.result().instanceId);
log.infof('rent contract %s: flow instance %s (%s)', uuid, instanceId, r.result().status);

// 4. the instance row the review page and the workflow share (decision / final_status)
var row = new java.util.HashMap();
row.put('uuid', uuid);
row.put('workflow_instance_uuid', instanceId);
row.put('start_date', java.sql.Timestamp.from(java.time.Instant.now()));
db.create('rent_contracts_workflow_instances', 'uuid', row);
log.infof('rent contract %s: rent_contracts_workflow_instances row created for instance %s', uuid, instanceId);
