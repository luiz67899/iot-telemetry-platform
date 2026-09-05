from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class SensorRule(Base):
    __tablename__ = "sensor_rules"
    
    codigo_hardware: Mapped[str] = mapped_column(primary_key=True)
    umidade_minima: Mapped[float]
    temperatura_maxima: Mapped[float]