from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from sqlalchemy import select
from src.config.database import Session, get_db
from src.middlewares.auth import current_user, can_write
from src.models import Handover
from src.services import handovers
from src.services.audit import record
router = APIRouter(prefix='/handovers')


class ExportInput(BaseModel):
    dataset_id: int


@router.get('')
def listing(db=Depends(get_db), u=Depends(current_user)):
    rows = db.scalars(select(Handover).order_by(Handover.id.desc()).limit(100))
    return [handovers.handover_dict(h) for h in rows]


@router.get('/{id}')
def detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    h = db.get(Handover, id)
    if not h:
        raise HTTPException(404, '交接记录不存在')
    return handovers.handover_dict(h)


@router.post('/export', status_code=201)
def export(data: ExportInput, db=Depends(get_db), u=Depends(can_write)):
    h = handovers.export_package(db, data.dataset_id, u)
    record(db, u.email, 'handover.export', {'id': h.id, 'dataset': data.dataset_id, 'fingerprint': h.fingerprint})
    db.commit()
    return handovers.handover_dict(h)


@router.get('/{id}/download')
def download(id: int, db=Depends(get_db), u=Depends(current_user)):
    h = db.get(Handover, id)
    if not h:
        raise HTTPException(404, '交接记录不存在')
    if h.direction != 'export':
        raise HTTPException(409, '只有导出的交接包可以下载')
    path = handovers.export_path(h)
    return FileResponse(path, filename=h.filename, media_type='application/zip')


def _import_sync(body, filename, user_id, email):
    # 在线程池中执行，避免大包校验与落库阻塞事件循环。
    with Session() as db:
        user = type('Actor', (), {'id': user_id, 'email': email})()
        h, created = handovers.import_package(db, body, filename, user)
        if created and h.status == 'completed':
            record(db, email, 'handover.import', {'id': h.id, 'package_id': h.package_id, 'fingerprint': h.fingerprint})
            db.commit()
        return handovers.handover_dict(h)


@router.post('/import')
async def import_package(request: Request, filename: str = 'handover.zip', u=Depends(can_write)):
    body = await request.body()
    return await run_in_threadpool(_import_sync, body, filename, u.id, u.email)
