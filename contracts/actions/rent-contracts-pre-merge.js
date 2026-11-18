// Action rent-contracts-pre-merge — metadata_name: rent_contracts, condition: MERGE, phase: PRE (sync, before the write).
// - last_update = now on every update
// - status turning APPROUVED (the workflow's PUT): renders the contract PDF and stores it in snello documents (S3),
//   linked to the contract (documents.table_name/table_key) and referenced by rent_contracts.contract_document.
//   PRE on purpose: if storing fails the PUT answers 400 and the workflow fails visibly instead of approving silently.
var uuid = String(key);
var status = map.get('status');
log.infof('rent contract %s update: status=%s fields=%s', uuid, status, map.keySet());
// automatic fields: the admin form shows them as inputs (it cannot hide or lock a field), so whatever a user typed
// there is dropped here; contract_document is set below, only by the approval itself; review_url is copied
// from the workflow instance row by that table's PRE MERGE action
['creation_date', 'created_by', 'contract_document', 'review_url'].forEach(function (f) {
    if (map.containsKey(f)) { log.infof('rent contract %s: ignoring edit of automatic field %s', uuid, f); map.remove(f); }
});
map.put('last_update', java.sql.Timestamp.from(java.time.Instant.now()));

if (status != null && String(status) === 'APPROUVED') {
    var current = db.fetch(null, 'rent_contracts', 'uuid', uuid);
    var existing = current == null ? null : current.get('contract_document');
    if (existing != null && String(existing) !== '') {
        log.infof('rent contract %s: already archived as document %s, skipping', uuid, existing);
    } else {
        var pdf = prints.print('rent_contract', { uuid: uuid }, null);
        var docUuid = java.util.UUID.randomUUID().toString();
        var name = 'rent-contract-' + uuid + '.pdf';
        var stored = documents.write(pdf.bytes(), docUuid, 'rent_contracts', 'pdf');
        log.infof('rent contract %s: pdf %s bytes stored at %s', uuid, pdf.bytes().length, stored.get('path'));
        var doc = new java.util.HashMap();
        doc.put('uuid', docUuid); doc.put('name', name); doc.put('original_name', name);
        doc.put('path', stored.get('path')); doc.put('mimetype', 'application/pdf'); doc.put('size', pdf.bytes().length);
        doc.put('table_name', 'rent_contracts'); doc.put('table_key', uuid);
        db.create('documents', 'uuid', doc);
        map.put('contract_document', docUuid);
        log.infof('rent contract %s: archived contract document %s', uuid, docUuid);
    }
}
