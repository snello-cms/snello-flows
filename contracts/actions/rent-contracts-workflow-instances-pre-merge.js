// Action rent-contracts-workflow-instances-pre-merge — metadata_name: rent_contracts_workflow_instances,
// condition: MERGE, phase: PRE (sync, before the write). Server-side clock and the "closed" guard:
// - decision set (ACCEPT / REFUSE) by the review page -> decision_date = now; cleared (UNDO) -> decision_date = null
//   (the workflow commits a decision only if decision + decision_date are unchanged after its grace period)
// - final_status set (the workflow's closing PUT) -> end_date = now
// - review_url set (the workflow, right after mailing the reviewer) -> copied to rent_contracts.review_url
// - once final_status is stored, decision changes are refused (400): UNDO after the workflow closed is not possible
var uuid = String(key);
var current = db.fetch(null, 'rent_contracts_workflow_instances', 'uuid', uuid);
var closed = current != null && current.get('final_status') != null && String(current.get('final_status')) !== '';
log.infof('workflow instance row %s update: fields=%s decision=%s final_status=%s closed=%s',
    uuid, map.keySet(), map.get('decision'), map.get('final_status'), closed);
var now = java.sql.Timestamp.from(java.time.Instant.now());

if (map.containsKey('decision')) {
    if (closed) {
        throw 'contract ' + uuid + ' is already closed as ' + current.get('final_status') + ': decision cannot change';
    }
    var d = map.get('decision');
    var set = d != null && String(d) !== '';
    if (!set) map.put('decision', null);
    map.put('decision_date', set ? now : null);
    log.infof('workflow instance row %s: decision=%s decision_date=%s', uuid, set ? d : null, set ? now : null);
}
if (map.containsKey('review_url')) {
    // the workflow writes the emailed review link here; the contract page shows it too
    var copied = db.query('UPDATE rent_contracts SET review_url = ? WHERE uuid = ?',
        java.util.List.of(map.get('review_url') == null ? '' : String(map.get('review_url')), uuid));
    log.infof('workflow instance row %s: review_url copied to rent_contracts: %s', uuid, copied);
}
if (map.get('final_status') != null && String(map.get('final_status')) !== '') {
    map.put('end_date', now);
    log.infof('workflow instance row %s: closing as %s at %s', uuid, map.get('final_status'), now);
}
