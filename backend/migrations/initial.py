from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models

# 新增列保持幂等，已有数据库卷无需清空即可升级。
_ADDED_COLUMNS = {
    'datasets': [('handover_id', 'INTEGER'), ('origin_id', 'INTEGER')],
    'jobs': [('handover_id', 'INTEGER'), ('origin_id', 'INTEGER')],
    'reports': [('handover_id', 'INTEGER'), ('origin_id', 'INTEGER')],
    'deliveries': [('handover_id', 'INTEGER'), ('origin_id', 'INTEGER'), ('origin_filename', 'VARCHAR(160)')],
}

def _ensure_columns():
    inspector = inspect(engine)
    if 'handovers' not in inspector.get_table_names():
        return
    with engine.begin() as connection:
        for table, columns in _ADDED_COLUMNS.items():
            existing = {c['name'] for c in inspector.get_columns(table)}
            for name, ddl in columns:
                if name not in existing:
                    connection.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))
        # 旧库的表已存在，create_all 不会补约束；幂等创建用于防重复迁入。
        for table in _ADDED_COLUMNS:
            connection.execute(text(
                f'CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_handover_origin ON {table} (handover_id, origin_id)'))

def migrate():
    Base.metadata.create_all(engine)
    _ensure_columns()
if __name__=='__main__':migrate()
