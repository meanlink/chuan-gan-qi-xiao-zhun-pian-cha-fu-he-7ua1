import hashlib, json, os
from src.config.settings import ARTIFACT_DIR
def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
def dataset_dict(d):
    return {'id':d.id, 'name':d.name, 'source':d.source, 'rows':len(d.records), 'checksum':d.checksum, 'created_at':d.created_at, 'handover_id':d.handover_id, 'origin_id':d.origin_id}
def job_dict(j):
    return {k:getattr(j,k) for k in ['id','name','dataset_id','params','status','result','error','approved','created_at','finished_at','handover_id','origin_id']}
def report_dict(r):
    return {k:getattr(r,k) for k in ['id','name','job_id','destination','interval_seconds','next_run','enabled','handover_id','origin_id']}
def delivery_dict(d):
    data={k:getattr(d,k) for k in ['id','report_id','created_at','checksum','filename','destination','handover_id','origin_id','origin_filename']}
    data['json_available']=os.path.isfile(os.path.join(ARTIFACT_DIR,d.filename+'.json'))
    return data
