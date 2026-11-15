"""批次交接包：导出样本、已完成分析、简报计划与归档；导入时整包校验、整组落库。

包格式（ZIP）：
  manifest.json   业务实体 + 每份归档文件的 SHA256
  files/...       归档原始字节（CSV 及其 JSON 伴随文件）

导入采用“先写文件、后单事务落库”的顺序：进程在任意位置中断，都不会留下半组业务
记录；重试会覆盖同名的暂存文件并完成同一条 Handover。导入以 package_id 幂等，
并发导入同一包只会有一组数据。
"""
import hashlib
import io
import math
import json
import os
import re
import secrets
import time
import zipfile
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select, or_
from sqlalchemy.exc import IntegrityError

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models import Dataset, Delivery, Handover, Job, Report
from src.services.business import metadata
from src.utils.serialization import checksum

FORMAT = 'sensor-bootstrap.handover'
VERSION = 1
MANIFEST_NAME = 'manifest.json'
MAX_PACKAGE_BYTES = 32 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 96 * 1024 * 1024
MAX_FILES = 2000
CLAIM_STALE_SECONDS = 120
IN_PROGRESS = ('validating', 'importing', 'recovering')
TERMINAL = ('completed', 'failed')


class PackageError(Exception):
    def __init__(self, errors):
        self.errors = errors if isinstance(errors, list) else [str(errors)]


# ---------------------------------------------------------------- 当前可见性

def visible(model):
    """导入未完成的整组业务记录对现有页面不可见。"""
    return or_(
        model.handover_id.is_(None),
        model.handover_id.in_(select(Handover.id).where(Handover.status == 'completed')),
    )


# ---------------------------------------------------------------- 序列化

def handover_dict(h):
    return {
        'id': h.id,
        'direction': h.direction,
        'status': h.status,
        'package_id': h.package_id,
        'filename': h.filename,
        'fingerprint': h.fingerprint,
        'dataset_id': h.dataset_id,
        'summary': h.summary or {},
        'errors': h.errors or [],
        'id_map': h.id_map or {},
        'created_at': h.created_at,
        'updated_at': h.updated_at,
    }


def _canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def _sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _safe_filename(name):
    base = os.path.basename(str(name or 'handover.zip')).replace('\\', '_')
    return re.sub(r'[^0-9A-Za-z._\-一-鿿]', '_', base)[:160] or 'handover.zip'


def _artifact_root():
    return Path(ARTIFACT_DIR) / 'handovers'


# ---------------------------------------------------------------- 导出

def export_package(db, dataset_id, user):
    dataset = db.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(404, '数据批次不存在')
    if dataset.handover_id is not None and not _handover_completed(db, dataset.handover_id):
        raise HTTPException(409, '该批次所属的迁入尚未完成，暂不能导出')

    jobs = list(db.scalars(select(Job).where(Job.dataset_id == dataset.id, Job.status == 'completed').order_by(Job.id)))
    job_ids = [j.id for j in jobs]
    reports = list(db.scalars(select(Report).where(Report.job_id.in_(job_ids)).order_by(Report.id))) if job_ids else []
    report_ids = [r.id for r in reports]
    deliveries = list(db.scalars(select(Delivery).where(Delivery.report_id.in_(report_ids)).order_by(Delivery.id))) if report_ids else []

    payload = {}
    manifest_files = []
    manifest_deliveries = []
    for delivery in deliveries:
        csv_path = Path(ARTIFACT_DIR) / delivery.filename
        if not csv_path.is_file():
            raise HTTPException(409, f'归档 {delivery.filename} 的文件缺失，无法生成完整交接包')
        csv_bytes = csv_path.read_bytes()
        if _sha_bytes(csv_bytes) != delivery.checksum:
            raise HTTPException(409, f'归档 {delivery.filename} 校验失败，无法生成完整交接包')
        csv_entry = f'files/delivery-{delivery.id}.csv'
        payload[csv_entry] = csv_bytes
        manifest_files.append({'path': csv_entry, 'sha256': delivery.checksum, 'size': len(csv_bytes), 'delivery_origin_id': delivery.id, 'format': 'csv'})
        entry = {'origin_id': delivery.id, 'report_origin_id': delivery.report_id, 'created_at': delivery.created_at,
                 'checksum': delivery.checksum, 'destination': delivery.destination, 'origin_filename': delivery.filename,
                 'files': [csv_entry]}
        json_path = Path(str(csv_path) + '.json')
        if json_path.is_file():
            json_bytes = json_path.read_bytes()
            json_entry = f'files/delivery-{delivery.id}.json'
            payload[json_entry] = json_bytes
            manifest_files.append({'path': json_entry, 'sha256': _sha_bytes(json_bytes), 'size': len(json_bytes), 'delivery_origin_id': delivery.id, 'format': 'json'})
            entry['files'].append(json_entry)
        manifest_deliveries.append(entry)

    package_id = secrets.token_hex(16)
    try:
        manifest = {
            'format': FORMAT,
            'version': VERSION,
            'package_id': package_id,
            'created_at': time.time(),
            'dataset': {'origin_id': dataset.id, 'name': dataset.name, 'source': dataset.source,
                        'checksum': dataset.checksum, 'created_at': dataset.created_at, 'records': dataset.records},
            'jobs': [{'origin_id': j.id, 'dataset_origin_id': dataset.id, 'name': j.name, 'params': j.params,
                      'status': j.status, 'result': j.result, 'error': j.error, 'approved': bool(j.approved),
                      'created_at': j.created_at, 'finished_at': j.finished_at} for j in jobs],
            'reports': [{'origin_id': r.id, 'job_origin_id': r.job_id, 'name': r.name, 'destination': r.destination,
                         'interval_seconds': r.interval_seconds, 'next_run': r.next_run, 'enabled': bool(r.enabled)} for r in reports],
            'deliveries': manifest_deliveries,
            'files': manifest_files,
        }
        manifest_bytes = _canonical_bytes(manifest)
    except (TypeError, ValueError):
        raise HTTPException(409, '批次内存在无法序列化为严格 JSON 的结果，无法生成交接包')
    blob = _build_zip(manifest_bytes, payload)

    h = Handover(direction='export', status='completed', package_id=package_id, fingerprint=_sha_bytes(blob),
                 filename=f'handover-{package_id[:12]}.zip', created_by=user.id, created_at=time.time(),
                 updated_at=time.time(), dataset_id=dataset.id,
                 summary={'dataset': 1, 'jobs': len(jobs), 'reports': len(reports), 'deliveries': len(deliveries),
                          'dataset_name': dataset.name},
                 errors=[], id_map={'dataset': {str(dataset.id): dataset.id},
                                    'jobs': {str(j.id): j.id for j in jobs},
                                    'reports': {str(r.id): r.id for r in reports},
                                    'deliveries': {str(d.id): d.id for d in deliveries}})
    db.add(h)
    db.flush()
    root = _artifact_root()
    root.mkdir(parents=True, exist_ok=True)
    _atomic_write(root / h.filename, blob)
    return h


def _build_zip(manifest_bytes, payload):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in [(MANIFEST_NAME, manifest_bytes)] + sorted(payload.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, data)
    return buffer.getvalue()


def export_path(h):
    root = _artifact_root().resolve()
    path = (root / h.filename).resolve()
    if root not in path.parents or not path.is_file():
        raise HTTPException(404, '交接包文件不存在，请重新导出')
    return path


# ---------------------------------------------------------------- 导入入口

def import_package(db, body, filename, user):
    if not body:
        raise HTTPException(422, '交接包为空，请选择 ZIP 文件')
    if len(body) > MAX_PACKAGE_BYTES:
        raise HTTPException(413, '交接包超过 32 MB，请按批次拆分')

    file_hash = _sha_bytes(body)
    package_id = _peek_package_id(body)
    import_key = (package_id or file_hash)[:80]
    h, token = _acquire(db, import_key, _safe_filename(filename), file_hash, user)
    if token is None:
        if h.status in TERMINAL:
            return h, False
        h = _await_terminal(db, h)
        return h, False
    return _run_import(db, h, token, body, user), True


def _peek_package_id(body):
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            manifest = json.loads(archive.read(MANIFEST_NAME).decode('utf-8'))
        value = manifest.get('package_id')
        if isinstance(value, str) and 8 <= len(value) <= 64:
            return value
    except Exception:
        return None
    return None


def _acquire(db, import_key, filename, file_hash, user):
    token = secrets.token_hex(12)
    for _ in range(3):
        now = time.time()
        h = db.scalar(select(Handover).where(Handover.import_key == import_key).with_for_update())
        if h is None:
            h = Handover(direction='import', status='validating', import_key=import_key, package_id=import_key if len(import_key) >= 8 else None,
                         fingerprint=file_hash, filename=filename, created_by=user.id, created_at=now, updated_at=now,
                         summary={}, errors=[], id_map={}, claim_token=token, claim_at=now, claim_owner=user.email)
            db.add(h)
            try:
                db.flush()
                db.commit()
                return h, token
            except IntegrityError:
                db.rollback()
                continue
        if h.status == 'completed':
            if h.fingerprint and h.fingerprint != file_hash:
                db.commit()
                raise HTTPException(409, '同一交接包编号的内容不一致，已拒绝导入；请重新导出交接包')
            db.commit()
            return h, None
        fresh = bool(h.claim_at) and now - h.claim_at < CLAIM_STALE_SECONDS
        if h.status in IN_PROGRESS and fresh and h.claim_token:
            db.commit()
            return h, None
        # 上次进程中断或校验失败：接管并重新执行，不新增第二组。
        h.status = 'validating'
        h.errors = []
        h.id_map = {}
        h.dataset_id = None
        h.claim_token = token
        h.claim_at = now
        h.claim_owner = user.email
        h.fingerprint = file_hash
        h.filename = filename
        h.updated_at = now
        db.commit()
        return h, token
    h = db.scalar(select(Handover).where(Handover.import_key == import_key))
    if h is None:
        raise HTTPException(409, '交接包正在被其他导入占用，请稍后重试')
    db.commit()
    return h, None


def _await_terminal(db, h, tries=150):
    for _ in range(tries):
        if h.status in TERMINAL:
            return h
        if h.status == 'recovering':
            break
        time.sleep(0.2)
        db.expire_all()
        h = db.get(Handover, h.id)
        if h is None:
            raise HTTPException(409, '交接包记录已不存在，请重试')
    db.expire_all()
    return db.get(Handover, h.id) or h


def _fail(db, h, errors, status='failed'):
    h.errors = list(errors)
    h.status = status
    h.claim_token = None
    h.claim_at = None
    h.updated_at = time.time()
    db.commit()
    return h


def _run_import(db, h, token, body, user):
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            manifest, file_data = _validate_package(archive)
    except PackageError as error:
        return _fail(db, h, error.errors)
    except (zipfile.BadZipFile, OSError, ValueError, UnicodeDecodeError):
        return _fail(db, h, ['交接包不是有效的 ZIP 文件或内容无法读取'])
    except Exception:
        return _fail(db, h, ['交接包校验异常，请确认文件来源后重试'])

    if not h.package_id:
        h.package_id = manifest['package_id']
    h.filename = _safe_filename(h.filename)
    h.status = 'importing'
    h.updated_at = time.time()
    db.commit()

    # 先写文件；任何中断都只会留下可用同包重试覆盖的暂存文件，不会产生半组记录。
    try:
        written = _write_archive_files(h, manifest, file_data)
    except PackageError as error:
        return _fail(db, h, error.errors)
    except OSError as error:
        return _fail(db, h, [f'归档写入失败：{error}'], status='recovering')
    except Exception:
        return _fail(db, h, ['归档写入中断，请重新导入同一交接包继续'], status='recovering')

    db.rollback()
    db.expire_all()
    current = db.scalar(select(Handover).where(Handover.id == h.id).with_for_update())
    if current is None or current.claim_token != token:
        db.commit()
        return current or h
    if current.status == 'completed':
        db.commit()
        return current

    now = time.time()
    try:
        dataset, counts, id_map = _insert_group(db, current, manifest, written, user, now)
    except IntegrityError:
        # 另一个并发导入抢先提交（PostgreSQL 下由行锁串行化，此处兜底其他数据库）。
        db.rollback()
        current = db.get(Handover, h.id)
        for _ in range(25):
            if current and current.status == 'completed':
                return current
            time.sleep(0.2)
            db.expire_all()
            current = db.get(Handover, h.id)
        return _fail(db, current or h, ['并发写入冲突，请稍后重试'], status='recovering')
    except HTTPException:
        db.rollback()
        raise
    except Exception:
        # 未提交的半组记录随事务回滚消失；标记为恢复中，重试同一包即可继续。
        db.rollback()
        current = db.get(Handover, h.id) or h
        return _fail(db, current, ['导入中断，请重新导入同一交接包继续'], status='recovering')

    current.status = 'completed'
    current.dataset_id = dataset.id
    current.summary = {**counts, 'dataset_name': dataset.name, 'renamed': dataset.name != manifest['dataset']['name']}
    current.id_map = id_map
    current.errors = []
    current.claim_token = None
    current.claim_at = None
    current.updated_at = time.time()
    db.commit()
    return current


# ---------------------------------------------------------------- 校验
def _record_ok(row):
    if not isinstance(row, dict):
        return False
    for column in metadata()['columns']:
        value = row.get(column['key'])
        if value is None or value == '':
            return False
        if column['type'] == 'number':
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return False
            if not math.isfinite(float(value)):
                return False
        elif not isinstance(value, str):
            return False
    return True



def _validate_package(archive):
    errors = []
    infos = archive.infolist()
    if len(infos) > MAX_FILES:
        raise PackageError(['交接包文件数量超出限制'])
    if sum(i.file_size for i in infos) > MAX_UNCOMPRESSED_BYTES:
        raise PackageError(['交接包解压后体积超出限制'])
    names = {i.filename for i in infos}
    if MANIFEST_NAME not in names:
        raise PackageError(['交接包缺少 manifest.json'])
    try:
        manifest = json.loads(archive.read(MANIFEST_NAME).decode('utf-8'))
    except (ValueError, UnicodeDecodeError):
        raise PackageError(['manifest.json 不是有效的 UTF-8 JSON'])
    if not isinstance(manifest, dict):
        raise PackageError(['manifest.json 结构不合法'])
    if manifest.get('format') != FORMAT:
        errors.append('交接包格式不受支持')
    if manifest.get('version') != VERSION:
        errors.append(f'交接包版本不受支持（支持版本 {VERSION}）')
    package_id = manifest.get('package_id')
    if not isinstance(package_id, str) or not 8 <= len(package_id) <= 64:
        errors.append('交接包缺少有效的 package_id')

    dataset = manifest.get('dataset')
    if not isinstance(dataset, dict):
        errors.append('交接包缺少样本批次')
        dataset = {}
    dataset_origin = dataset.get('origin_id')
    if not isinstance(dataset_origin, int) or isinstance(dataset_origin, bool):
        errors.append('样本批次缺少原始编号')
    name = dataset.get('name')
    if not isinstance(name, str) or not 2 <= len(name.strip()) <= 160:
        errors.append('样本批次名称不合法')
    records = dataset.get('records')
    if not isinstance(records, list) or not 5 <= len(records) <= 5000:
        errors.append('样本批次记录数须为 5–5000 条')
    else:
        try:
            declared = dataset.get('checksum')
            actual = checksum(records)
            if not isinstance(declared, str) or declared != actual:
                errors.append('样本内容与 manifest 中记录的校验值不一致')
        except (TypeError, ValueError):
            errors.append('样本记录不是有效的有限数值 JSON')
        else:
            bad_rows = sum(1 for row in records if not _record_ok(row))
            if bad_rows:
                errors.append(f'样本记录有 {bad_rows} 行缺少字段或读数不是有限数值')
    if not isinstance(dataset.get('source'), str):
        errors.append('样本批次来源不合法')
    if not isinstance(dataset.get('created_at'), (int, float)):
        errors.append('样本批次创建时间不合法')

    jobs = manifest.get('jobs')
    if not isinstance(jobs, list):
        errors.append('缺少分析任务列表')
        jobs = []
    job_ids = set()
    for index, job in enumerate(jobs):
        label = f'分析任务第 {index + 1} 项'
        if not isinstance(job, dict):
            errors.append(f'{label}结构不合法')
            continue
        origin = job.get('origin_id')
        if not isinstance(origin, int) or isinstance(origin, bool):
            errors.append(f'{label}缺少原始编号')
            continue
        if origin in job_ids:
            errors.append(f'{label}原始编号重复')
        job_ids.add(origin)
        if job.get('dataset_origin_id') != dataset_origin:
            errors.append(f'{label}引用了不存在的样本批次')
        if job.get('status') != 'completed':
            errors.append(f'{label}不是已完成分析')
        if not isinstance(job.get('name'), str) or not 2 <= len(job.get('name')) <= 160:
            errors.append(f'{label}名称不合法')
        if not isinstance(job.get('params'), dict) or not isinstance(job.get('result'), dict):
            errors.append(f'{label}缺少参数或结果快照')
        if not isinstance(job.get('approved'), bool):
            errors.append(f'{label}批准状态不合法')
        for key in ('created_at', 'finished_at'):
            if not isinstance(job.get(key), (int, float)) and not (key == 'finished_at' and job.get(key) is None):
                errors.append(f'{label}时间字段不合法')
        try:
            _canonical_bytes(job.get('result'))
            _canonical_bytes(job.get('params'))
        except (TypeError, ValueError):
            errors.append(f'{label}结果无法序列化为有限数值 JSON')

    reports = manifest.get('reports')
    if not isinstance(reports, list):
        errors.append('缺少简报计划列表')
        reports = []
    report_ids = set()
    for index, report in enumerate(reports):
        label = f'简报计划第 {index + 1} 项'
        if not isinstance(report, dict):
            errors.append(f'{label}结构不合法')
            continue
        origin = report.get('origin_id')
        if not isinstance(origin, int) or isinstance(origin, bool):
            errors.append(f'{label}缺少原始编号')
            continue
        if origin in report_ids:
            errors.append(f'{label}原始编号重复')
        report_ids.add(origin)
        if report.get('job_origin_id') not in job_ids:
            errors.append(f'{label}引用了不存在的分析任务')
        if not isinstance(report.get('name'), str) or not 2 <= len(report.get('name')) <= 160:
            errors.append(f'{label}名称不合法')
        if not isinstance(report.get('destination'), str) or not 2 <= len(report.get('destination')) <= 120:
            errors.append(f'{label}接收组不合法')
        interval = report.get('interval_seconds')
        if not isinstance(interval, int) or isinstance(interval, bool) or not 60 <= interval <= 2592000:
            errors.append(f'{label}分发周期不合法')
        if not isinstance(report.get('enabled'), bool):
            errors.append(f'{label}启用状态不合法')

    deliveries = manifest.get('deliveries')
    if not isinstance(deliveries, list):
        errors.append('缺少归档列表')
        deliveries = []
    delivery_ids = set()
    for index, delivery in enumerate(deliveries):
        label = f'归档第 {index + 1} 项'
        if not isinstance(delivery, dict):
            errors.append(f'{label}结构不合法')
            continue
        origin = delivery.get('origin_id')
        if not isinstance(origin, int) or isinstance(origin, bool):
            errors.append(f'{label}缺少原始编号')
            continue
        if origin in delivery_ids:
            errors.append(f'{label}原始编号重复')
        delivery_ids.add(origin)
        if delivery.get('report_origin_id') not in report_ids:
            errors.append(f'{label}引用了不存在的简报计划')
        declared = delivery.get('checksum')
        if not isinstance(declared, str) or not re.fullmatch(r'[0-9a-f]{64}', declared):
            errors.append(f'{label}校验值不合法')
        if not isinstance(delivery.get('destination'), str):
            errors.append(f'{label}接收组不合法')
        if not isinstance(delivery.get('created_at'), (int, float)):
            errors.append(f'{label}时间字段不合法')
        if not isinstance(delivery.get('files'), list) or not delivery.get('files'):
            errors.append(f'{label}缺少归档文件引用')

    file_entries = manifest.get('files')
    if not isinstance(file_entries, list):
        errors.append('缺少归档文件清单')
        file_entries = []
    seen_paths = set()
    for index, entry in enumerate(file_entries):
        label = f'文件清单第 {index + 1} 项'
        if not isinstance(entry, dict):
            errors.append(f'{label}结构不合法')
            continue
        path = entry.get('path')
        if not isinstance(path, str) or not path or path.startswith('/') or '\\' in path or '..' in path.split('/'):
            errors.append(f'{label}路径不安全')
            continue
        if path == MANIFEST_NAME or path in seen_paths:
            errors.append(f'{label}路径重复或与清单冲突')
            continue
        seen_paths.add(path)
        declared = entry.get('sha256')
        if not isinstance(declared, str) or not re.fullmatch(r'[0-9a-f]{64}', declared):
            errors.append(f'{label}校验值不合法')
        size = entry.get('size')
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            errors.append(f'{label}大小不合法')
        owner = entry.get('delivery_origin_id')
        if owner not in delivery_ids:
            errors.append(f'{label}未关联到有效归档')
            continue
        if entry.get('format') not in ('csv', 'json'):
            errors.append(f'{label}文件类型不合法')

    # 引用缺失即整包拒绝：每份归档都必须有 CSV，且所有清单文件都真实存在并校验通过。
    file_data = {}
    for delivery in deliveries:
        if not isinstance(delivery, dict) or delivery.get('origin_id') not in delivery_ids:
            continue
        refs = delivery.get('files') if isinstance(delivery.get('files'), list) else []
        if not any(p in seen_paths for p in refs if isinstance(p, str)):
            errors.append(f'归档 {delivery.get("origin_id")} 的引用文件缺失')
        csv_refs = [p for p in refs if isinstance(p, str) and p.endswith('.csv')]
        if not csv_refs:
            errors.append(f'归档 {delivery.get("origin_id")} 缺少 CSV 原件')
        for path in refs:
            if not isinstance(path, str) or path not in names:
                errors.append(f'归档 {delivery.get("origin_id")} 引用的文件在包内缺失')
            elif path not in file_data:
                data = archive.read(path)
                file_data[path] = data

    for entry in file_entries:
        if not isinstance(entry, dict):
            continue
        path = entry.get('path')
        if not isinstance(path, str) or path not in file_data:
            continue
        data = file_data[path]
        if len(data) != entry.get('size'):
            errors.append(f'文件 {path} 大小与清单不一致')
        if _sha_bytes(data) != entry.get('sha256'):
            errors.append(f'文件 {path} 校验失败')

    # 每份归档声明的 checksum 必须与其 CSV 实际内容一致。
    for delivery in deliveries:
        if not isinstance(delivery, dict):
            continue
        origin = delivery.get('origin_id')
        csv_refs = [p for p in delivery.get('files', []) if isinstance(p, str) and p.endswith('.csv')] if isinstance(delivery.get('files'), list) else []
        for path in csv_refs:
            data = file_data.get(path)
            if data is not None and delivery.get('checksum') and _sha_bytes(data) != delivery.get('checksum'):
                errors.append(f'归档 {origin} 的内容与声明校验值不一致')
                break

    if errors:
        raise PackageError(errors)
    return manifest, file_data


# ---------------------------------------------------------------- 落库

def _unique_dataset_name(db, name, handover_id):
    base = name.strip()
    if not db.scalar(select(Dataset.id).where(Dataset.name == base)):
        return base
    for index in [f'迁入 {handover_id}'] + [f'迁入 {handover_id}-{n}' for n in range(2, 1000)]:
        candidate = base[:150] + f'（{index}）'
        if not db.scalar(select(Dataset.id).where(Dataset.name == candidate)):
            return candidate
    raise HTTPException(409, '目标批次名称冲突过多，无法分配新名称')


def _insert_group(db, handover, manifest, written, user, now):
    source = manifest['dataset']
    name = _unique_dataset_name(db, source['name'], handover.id)
    dataset = Dataset(name=name, source=source.get('source') or 'handover.zip',
                      records=source['records'], checksum=checksum(source['records']),
                      created_at=now, handover_id=handover.id, origin_id=source['origin_id'])
    db.add(dataset)
    db.flush()

    id_map = {'dataset': {str(source['origin_id']): dataset.id}, 'jobs': {}, 'reports': {}, 'deliveries': {}}
    job_ids = {}
    for job in sorted(manifest['jobs'], key=lambda item: item['origin_id']):
        row = Job(name=job['name'], dataset_id=dataset.id, params=job['params'], status='completed',
                  result=job['result'], error=str(job.get('error') or '')[:990], approved=job['approved'],
                  created_by=user.id, created_at=job['created_at'], finished_at=job['finished_at'],
                  handover_id=handover.id, origin_id=job['origin_id'])
        db.add(row)
        db.flush()
        job_ids[job['origin_id']] = row.id
        id_map['jobs'][str(job['origin_id'])] = row.id

    report_ids = {}
    for report in sorted(manifest['reports'], key=lambda item: item['origin_id']):
        interval = report['interval_seconds']
        row = Report(name=report['name'], job_id=job_ids[report['job_origin_id']], destination=report['destination'],
                     interval_seconds=interval, next_run=now + interval, enabled=False,
                     handover_id=handover.id, origin_id=report['origin_id'])
        db.add(row)
        db.flush()
        report_ids[report['origin_id']] = row.id
        id_map['reports'][str(report['origin_id'])] = row.id

    for delivery in sorted(manifest['deliveries'], key=lambda item: item['origin_id']):
        origin = delivery['origin_id']
        filename = written.get(f'{origin}:csv') or f'handover-{handover.id}-delivery-{origin}.csv'
        row = Delivery(report_id=report_ids[delivery['report_origin_id']], created_at=delivery['created_at'],
                       checksum=delivery['checksum'], filename=filename, destination=delivery['destination'],
                       handover_id=handover.id, origin_id=origin,
                       origin_filename=str(delivery.get('origin_filename') or '')[:160] or None)
        db.add(row)
        db.flush()
        id_map['deliveries'][str(origin)] = row.id

    return dataset, {'dataset': 1, 'jobs': len(manifest['jobs']), 'reports': len(manifest['reports']),
                     'deliveries': len(manifest['deliveries'])}, id_map


def _write_archive_files(handover, manifest, file_data):
    """把归档字节写到下载目录。同名重试直接覆盖，保证不产生重复文件。"""
    directory = Path(ARTIFACT_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    written = {}
    for delivery in manifest['deliveries']:
        origin = delivery['origin_id']
        base = f'handover-{handover.id}-delivery-{origin}'
        for path in delivery['files']:
            data = file_data[path]
            if path.endswith('.csv'):
                target = directory / f'{base}.csv'
                if _sha_bytes(data) != delivery['checksum']:
                    raise PackageError([f'归档 {origin} 的内容与声明校验值不一致'])
                _atomic_write(target, data)
                written[f'{origin}:csv'] = target.name
            elif path.endswith('.json'):
                target = directory / f'{base}.csv.json'
                _atomic_write(target, data)
                written[f'{origin}:json'] = target.name
    return written


def _atomic_write(path, data):
    path = Path(path)
    temp = path.with_name(path.name + f'.tmp-{secrets.token_hex(6)}')
    with open(temp, 'wb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def _handover_completed(db, handover_id):
    h = db.get(Handover, handover_id)
    return bool(h and h.status == 'completed')


# ---------------------------------------------------------------- 启动恢复

def reconcile(grace_seconds=60):
    """启动时把上次进程遗留的进行中导入标为恢复中；事务未提交的业务记录本就不可见。

    启动点调用时传 grace_seconds=0：此刻数据库中的进行中记录必然来自已退出的进程。
    """
    now = time.time()
    with Session() as db:
        rows = list(db.scalars(select(Handover).where(Handover.status.in_(('validating', 'importing')))))
        owned_elsewhere = [h for h in rows if grace_seconds and h.claim_at and now - h.claim_at < grace_seconds]
        rows = [h for h in rows if h not in owned_elsewhere]
        for h in rows:
            h.status = 'recovering'
            h.claim_token = None
            h.claim_at = None
            h.updated_at = time.time()
        if rows:
            db.commit()
        return len(rows)
