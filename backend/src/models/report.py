from sqlalchemy import String, Double, ForeignKey, Boolean, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base
class Report(Base):
    __tablename__ = 'reports'
    __table_args__ = (UniqueConstraint('handover_id', 'origin_id', name='uq_reports_handover_origin'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'))
    destination: Mapped[str] = mapped_column(String(120))
    interval_seconds: Mapped[int]
    next_run: Mapped[float] = mapped_column(Double)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    handover_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    origin_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
class Delivery(Base):
    __tablename__ = 'deliveries'
    __table_args__ = (UniqueConstraint('handover_id', 'origin_id', name='uq_deliveries_handover_origin'),)
    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey('reports.id'))
    created_at: Mapped[float] = mapped_column(Double)
    checksum: Mapped[str] = mapped_column(String(64))
    filename: Mapped[str] = mapped_column(String(160))
    destination: Mapped[str] = mapped_column(String(120))
    handover_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    origin_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    origin_filename: Mapped[str | None] = mapped_column(String(160), nullable=True)
