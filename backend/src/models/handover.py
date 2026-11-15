from sqlalchemy import String, JSON, Double, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class Handover(Base):
    """一次交接包导出或导入。导入期间整组业务记录不可见。"""
    __tablename__ = 'handovers'
    id: Mapped[int] = mapped_column(primary_key=True)
    direction: Mapped[str] = mapped_column(String(12), default='export')
    status: Mapped[str] = mapped_column(String(30), default='completed')
    package_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    import_key: Mapped[str | None] = mapped_column(String(80), unique=True, nullable=True)
    fingerprint: Mapped[str] = mapped_column(String(64), default='')
    filename: Mapped[str] = mapped_column(String(200), default='')
    dataset_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_by: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[float] = mapped_column(Double)
    updated_at: Mapped[float] = mapped_column(Double)
    summary: Mapped[dict] = mapped_column(JSON, default=dict)
    errors: Mapped[list] = mapped_column(JSON, default=list)
    id_map: Mapped[dict] = mapped_column(JSON, default=dict)
    claim_token: Mapped[str | None] = mapped_column(String(40), nullable=True)
    claim_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    claim_owner: Mapped[str | None] = mapped_column(String(120), nullable=True)
